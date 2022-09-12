//! A generic states manager for components live-upgrade and failover.
//! It utilizes Firecracker Snapshot crate to serialize, deserialize, calculate CRC and add Magic header.
//! In addition, it send process Fd to other process, thus to dup fd between processes to prevent a
//! file from being closed when the process exits.
//! Thus it can provide a trustable process states persistence mechanism.

#[macro_use]
extern crate log;

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::os::unix::prelude::{AsRawFd, FromRawFd};
use std::sync::{Arc, Mutex};

use snapshot::{Error as SnapshotErr, Persist, Snapshot};
use versionize::{VersionMap, Versionize, VersionizeResult};
use versionize_derive::Versionize;

pub mod states_storage;
use states_storage::StatesStorage;

const STATE_MAX_BUF_LEN: usize = 1024;
// All managed states are filled into the buffer, so 256KB should be enough on heap
const MANAGER_STATE_MAX_BUF_LEN: usize = 0x40000;

#[derive(Debug)]
pub enum UpgradeManagerError {
    Snapshot(SnapshotErr),
    Storage(io::Error),
    IO(io::Error),
    CloneFile(io::Error),
    Restore(String),
}

pub type UpgradeManagerResult<T> = std::result::Result<T, UpgradeManagerError>;

trait State: Read + Seek {}

impl<T: AsRef<[u8]>> State for Cursor<T> {}

pub struct UpgradeManager {
    vm: VersionMap,
    states: HashMap<String, Arc<Mutex<dyn State + Sync + Send>>>,
    storage: Arc<dyn StatesStorage>,
    file: Option<File>,
}

#[derive(Versionize)]
struct ManagedState {
    id: String,
    state: Vec<u8>,
}

#[derive(Versionize)]
pub struct UpgradeManagerState {
    states: Vec<ManagedState>,
}

pub struct UpgradeManagerConstructorArgs {
    vm: VersionMap,
    storage: Arc<dyn StatesStorage>,
    file: Option<File>,
}

impl Persist<'_> for UpgradeManager {
    type State = UpgradeManagerState;
    type ConstructorArgs = UpgradeManagerConstructorArgs;
    type Error = UpgradeManagerError;

    fn save(&self) -> Self::State {
        let states = self
            .states
            .iter()
            .filter_map(|(k, v)| {
                let mut t = vec![0u8; STATE_MAX_BUF_LEN];
                // Not expected dead lock here
                let mut guard = v.lock().unwrap();

                guard.seek(SeekFrom::Start(0)).unwrap();

                guard
                    .read(t.as_mut_slice())
                    .map(|l| ManagedState {
                        id: k.clone(),
                        state: t[0..l].to_owned(),
                    })
                    .map_err(|e| {
                        error!("Failed to extract state, this is serious problem, {}", e);
                        e
                    })
                    .ok()
            })
            .collect();

        UpgradeManagerState { states }
    }

    fn restore(
        constructor_args: Self::ConstructorArgs,
        state: &Self::State,
    ) -> std::result::Result<Self, Self::Error> {
        let mut um = Self::new(constructor_args.vm, constructor_args.storage);

        state.states.iter().for_each(|s| {
            um.states.insert(
                s.id.clone(),
                Arc::new(Mutex::new(Cursor::new(s.state.to_vec()))),
            );
        });

        um.file = constructor_args.file;

        Ok(um)
    }
}

impl UpgradeManager {
    pub fn new(vm: VersionMap, storage: Arc<dyn StatesStorage>) -> Self {
        Self {
            vm,
            states: HashMap::new(),
            storage,
            file: None,
        }
    }

    pub fn get_version_map(&self) -> VersionMap {
        self.vm.clone()
    }

    /// Save a single kind of state by snapshot(serialized and CRCed)
    pub fn save_state<O: Versionize>(&mut self, id: &str, state: &O) -> UpgradeManagerResult<()> {
        let vm = self.vm.clone();
        let latest_version = vm.latest_version();
        let mut cursor = Cursor::new(Vec::new());
        let mut snapshot = Snapshot::new(vm, latest_version);
        snapshot
            .save(&mut cursor, state)
            .map_err(UpgradeManagerError::Snapshot)?;

        self.states
            .insert(id.to_string(), Arc::new(Mutex::new(cursor)));

        Ok(())
    }

    /// Remove a state which has no chance to be persisted anymore
    pub fn remove_state(&mut self, id: &str) {
        if self.states.remove(id).is_none() {
            warn!("ID {}: state was not saved before!", id)
        }
    }

    pub fn list_all_states_ids(&self) -> Vec<String> {
        self.states.keys().cloned().collect()
    }

    /// Restore a single kind of state from its snapshot without touching other kinds of states.
    /// Caller should load its stored state and call `Snapshot::load` to recover Persisted state
    pub fn restore_state(&self, state_id: &str) -> UpgradeManagerResult<Option<Vec<u8>>> {
        if let Some(s) = self.states.get(state_id) {
            // Not expected poisoned here.
            let mut reader = s.lock().unwrap();
            let mut buf = vec![0u8; STATE_MAX_BUF_LEN];
            let size = reader
                .read(buf.as_mut_slice())
                .map_err(UpgradeManagerError::IO)?;

            debug!("restoring state size {}", size);

            Ok(Some(buf[0..size].to_owned()))
        } else {
            Ok(None)
        }
    }

    pub fn hold_file(&mut self, fd: &File) -> UpgradeManagerResult<()> {
        let f = fd.try_clone().map_err(UpgradeManagerError::CloneFile)?;
        self.file = Some(f);

        Ok(())
    }

    // NOTE:  Upgrade manager should never close this FILE unless it is called to do
    pub fn return_file(&mut self) -> Option<File> {
        if let Some(ref f) = self.file {
            // Basically, this can hardly fail.
            f.try_clone()
                .map_err(|e| {
                    error!("Clone file error, {}", e);
                    e
                })
                .ok()
        } else {
            warn!("No file can be returned");
            None
        }
    }

    pub fn swap_states(&mut self, m: Self) {
        self.states = m.states;
        self.file = m.file
        // Don't touch storage backend
    }

    /// Persist all states snapshots manage to external persistent storage
    pub fn persist_states(&self) -> UpgradeManagerResult<()> {
        let vm = self.vm.clone();
        let latest_version = vm.latest_version();
        let buf = vec![0u8; MANAGER_STATE_MAX_BUF_LEN];

        let all_states = self.save();

        let mut cursor = Cursor::new(buf);

        let mut snapshot = Snapshot::new(vm, latest_version);
        snapshot
            .save(&mut cursor, &all_states)
            .map_err(UpgradeManagerError::Snapshot)?;

        let tail = cursor.position() as usize;

        debug!("Sending all states, total size {}", tail);
        let mut fds = Vec::new();

        if let Some(ref f) = self.file {
            fds.push(f.as_raw_fd())
        }

        self.storage
            .write(&cursor.into_inner()[..tail], fds.as_slice())
            .map_err(UpgradeManagerError::Storage)
    }

    /// Fetch all states managed by `UpgradeManager` from storage backend and try
    /// to recover upgrade manager with all snapshots.
    pub fn fetch_states_and_restore(&self) -> UpgradeManagerResult<Self> {
        let vm = self.vm.clone();
        let mut buf = vec![0u8; MANAGER_STATE_MAX_BUF_LEN];
        let mut fds = vec![0i32; 16];
        let (snapshot_len, fds_cnt) = self
            .storage
            .read(buf.as_mut_slice(), fds.as_mut_slice())
            .map_err(UpgradeManagerError::Storage)?;

        let mut cursor = Cursor::new(buf);

        debug!(
            "Upgrade manager's snapshot size {} fds cnt {}",
            snapshot_len, fds_cnt
        );

        let state: UpgradeManagerState = Snapshot::load(&mut cursor, snapshot_len, vm.clone())
            .map_err(UpgradeManagerError::Snapshot)?;

        let f = if fds_cnt != 0 {
            if fds_cnt != 1 {
                warn!("Too many fds {}, we may not correctly handle it", fds_cnt);
            }
            Some(unsafe { File::from_raw_fd(fds[0]) })
        } else {
            None
        };

        let args = UpgradeManagerConstructorArgs {
            vm,
            storage: self.storage.clone(),
            file: f,
        };

        UpgradeManager::restore(args, &state)
    }
}
