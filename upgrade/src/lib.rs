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
                // Not expected dead lock here
                let mut guard = v.lock().unwrap();

                let mut t = vec![0u8; STATE_MAX_BUF_LEN];
                let mut state_buf = Vec::new();
                guard.seek(SeekFrom::Start(0)).unwrap();
                loop {
                    match guard.read(t.as_mut_slice()) {
                        Ok(size) => {
                            if size == 0 {
                                break Some(ManagedState {
                                    id: k.clone(),
                                    state: state_buf,
                                });
                            }
                            state_buf.extend(&t[..size]);
                        }
                        Err(e) => {
                            error!("Failed to extract state, this is serious problem, {}", e);
                            break None;
                        }
                    }
                }
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
            let mut state_buf = Vec::new();
            let mut buf = vec![0u8; STATE_MAX_BUF_LEN];
            loop {
                let size = reader
                    .read(buf.as_mut_slice())
                    .map_err(UpgradeManagerError::IO)?;
                if size == 0 {
                    break;
                }
                state_buf.extend(&buf[0..size]);
            }

            debug!("restoring state size {}", state_buf.len());

            Ok(Some(state_buf))
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

#[cfg(test)]
mod tests {

    use std::any::TypeId;
    use std::io::Cursor;
    use std::sync::Arc;

    use snapshot::{Error as SnapshotError, Snapshot};
    use versionize::{VersionMap, Versionize, VersionizeResult};
    use versionize_derive::Versionize;

    use super::UpgradeManagerError;
    use crate::{states_storage::mem_storage::MemoryStatesStorage, STATE_MAX_BUF_LEN};

    use super::UpgradeManager;

    impl SS2 {
        fn default_ss2_state_d(_target_version: u16) -> u64 {
            0x1234
        }
    }

    #[derive(Versionize, Clone)]
    struct SS1 {
        a: u32,
        b: String,
        c: f64,
    }

    #[derive(Versionize, Clone)]
    struct SS1NoVersionHint {
        a: u32,
        b: String,
        d: u64,
        c: f64,
    }

    #[derive(Versionize, Clone)]
    struct SS2 {
        a: u32,
        b: String,

        c: f64,

        // Lower version of SS won't deserialize the `d` added from version 2.
        // With version 1 VersionMap, higher version of program is using the default value.
        // `start` is the type version of `SS2`. It must be lower then VersionMap latest root version.
        #[version(start = 2, default_fn = "default_ss2_state_d")]
        d: u64,
    }

    #[test]
    fn test_upgrade_compatibility_higher_version_struct() {
        let vm1 = VersionMap::new();
        let states_storage = Arc::new(MemoryStatesStorage::new(1024 * 1024 * 8));
        let mut upgrade_manager1 = UpgradeManager::new(vm1, states_storage.clone());

        let ss1 = SS1 {
            a: 678,
            b: "abcde".to_string(),
            c: 1.89,
        };

        let ss1_state_id = "ss";

        upgrade_manager1.save_state(ss1_state_id, &ss1).unwrap();
        upgrade_manager1.persist_states().unwrap();

        // After upgrading

        let vm2 = VersionMap::new();
        let m = UpgradeManager::new(vm2.clone(), states_storage);
        let upgrade_manager2 = m.fetch_states_and_restore().unwrap();

        let s = upgrade_manager2
            .restore_state(ss1_state_id)
            .unwrap()
            .unwrap();

        let l = s.len();

        let mut cursor = Cursor::new(s);

        let ss2: SS2 = Snapshot::load(&mut cursor, l, vm2).unwrap();

        assert_eq!(ss2.a, 678);
        assert_eq!(ss2.d, 0x1234);
    }

    #[test]
    fn test_upgrade_compatibility_struct_version_equal_version_map() {
        let vm1 = VersionMap::new();
        let states_storage = Arc::new(MemoryStatesStorage::new(4096));
        let mut upgrade_manager1 = UpgradeManager::new(vm1, states_storage.clone());

        let ss1 = SS1 {
            a: 678,
            b: "abcde".to_string(),
            c: 1.89,
        };

        let ss1_state_id = "ss";

        upgrade_manager1.save_state(ss1_state_id, &ss1).unwrap();
        upgrade_manager1.persist_states().unwrap();

        // After upgrading

        let mut vm2 = VersionMap::new();
        vm2.new_version();

        let m = UpgradeManager::new(vm2.clone(), states_storage);
        let upgrade_manager2 = m.fetch_states_and_restore().unwrap();

        let s = upgrade_manager2
            .restore_state(ss1_state_id)
            .unwrap()
            .unwrap();

        let l = s.len();

        let mut cursor = Cursor::new(s);

        let ss2: SS2 = Snapshot::load(&mut cursor, l, vm2).unwrap();

        assert_eq!(ss2.a, 678);
        assert_eq!(ss2.d, 0x1234);
    }

    #[test]
    fn test_upgrade_compatibility_struct_version_gt_version_map() {
        let mut vm1 = VersionMap::new();
        // Version starts from 1, so a call to `new_version` increase it to 2.
        vm1.new_version();
        let states_storage = Arc::new(MemoryStatesStorage::new(1024 * 1024 * 8));
        let mut upgrade_manager1 = UpgradeManager::new(vm1, states_storage.clone());

        let ss2 = SS2 {
            a: 678,
            b: "abcde".to_string(),
            c: 1.89,
            d: 0x2323,
        };

        let ss2_state_id = "ss";

        upgrade_manager1.save_state(ss2_state_id, &ss2).unwrap();
        upgrade_manager1.persist_states().unwrap();

        // After upgrading

        let vm2 = VersionMap::new();

        let m = UpgradeManager::new(vm2, states_storage);

        // `Err` value: Snapshot(InvalidDataVersion(2))
        // Lower VersionMap can't decode struct serialized by higher version of VersionMap
        // TODO: Take this into consideration of downgrade rollback.
        match m.fetch_states_and_restore() {
            Ok(_) => panic!(),
            Err(UpgradeManagerError::Snapshot(SnapshotError::InvalidDataVersion(2))) => (),
            _ => panic!(),
        }
    }

    #[test]
    fn test_upgrade_compatibility_struct_no_version_hint() {
        let vm1 = VersionMap::new();
        let states_storage = Arc::new(MemoryStatesStorage::new(4096));
        let mut upgrade_manager1 = UpgradeManager::new(vm1, states_storage.clone());

        let ss1 = SS1 {
            a: 678,
            b: "abcde".to_string(),
            c: 1.89,
        };

        let ss1_state_id = "ss";

        upgrade_manager1.save_state(ss1_state_id, &ss1).unwrap();
        upgrade_manager1.persist_states().unwrap();

        // After upgrading

        let vm2 = VersionMap::new();

        let m = UpgradeManager::new(vm2.clone(), states_storage);
        let upgrade_manager2 = m.fetch_states_and_restore().unwrap();

        let s = upgrade_manager2
            .restore_state(ss1_state_id)
            .unwrap()
            .unwrap();

        let l = s.len();

        let mut cursor = Cursor::new(s);

        match Snapshot::load::<_, SS1NoVersionHint>(&mut cursor, l, vm2) {
            Err(SnapshotError::Versionize(_)) => (),
            _ => panic!(),
        }
    }

    #[test]
    fn test_upgrade_compatibility_root_version_type_version() {
        let mut vm1 = VersionMap::new();
        vm1.new_version();
        vm1.set_type_version(TypeId::of::<SS2>(), 2);
        let states_storage = Arc::new(MemoryStatesStorage::new(4096));
        let mut upgrade_manager1 = UpgradeManager::new(vm1, states_storage.clone());

        let ss2 = SS2 {
            a: 678,
            b: "abcde".to_string(),
            c: 1.89,
            // Field that starts from root version 2 won't be serialized.
            d: 0xabcd,
        };

        let ss2_state_id = "ss";

        upgrade_manager1.save_state(ss2_state_id, &ss2).unwrap();
        upgrade_manager1.persist_states().unwrap();

        // After upgrading

        let mut vm2 = VersionMap::new();
        // Version 2 Version map finds `SS2` version 2, will try to deserialize it
        // and find out that not enough data is serialized and persisted.
        vm2.new_version().set_type_version(TypeId::of::<SS2>(), 2);

        let m = UpgradeManager::new(vm2.clone(), states_storage);
        let upgrade_manager2 = m.fetch_states_and_restore().unwrap();

        let s = upgrade_manager2
            .restore_state(ss2_state_id)
            .unwrap()
            .unwrap();

        let l = s.len();

        let mut cursor = Cursor::new(s);

        let ss2: SS2 = Snapshot::load(&mut cursor, l, vm2).unwrap();

        assert_eq!(ss2.a, 678);
        // So version 2 version map uses the default value of `d`
        assert_eq!(ss2.d, 0xabcd);
    }

    #[derive(Versionize, Clone)]
    struct NestedSS2 {
        a: i32,
        ss2: SS2,
    }

    #[test]
    fn test_upgrade_compatibility_nested() {
        let mut vm1 = VersionMap::new();
        vm1.new_version();
        vm1.set_type_version(TypeId::of::<SS2>(), 2);
        let states_storage = Arc::new(MemoryStatesStorage::new(4096));
        let mut upgrade_manager1 = UpgradeManager::new(vm1, states_storage.clone());

        let ss_nested = NestedSS2 {
            a: 789,
            ss2: SS2 {
                a: 678,
                b: "abcde".to_string(),
                c: 1.89,
                // Field that starts from root version 2 won't be serialized.
                d: 0xabcd,
            },
        };

        let ss_nested_state_id = "ss";

        upgrade_manager1
            .save_state(ss_nested_state_id, &ss_nested)
            .unwrap();
        upgrade_manager1.persist_states().unwrap();

        // After upgrading

        let mut vm2 = VersionMap::new();
        vm2.new_version().set_type_version(TypeId::of::<SS2>(), 2);

        let m = UpgradeManager::new(vm2.clone(), states_storage);
        let upgrade_manager2 = m.fetch_states_and_restore().unwrap();

        let s = upgrade_manager2
            .restore_state(ss_nested_state_id)
            .unwrap()
            .unwrap();

        let l = s.len();

        let mut cursor = Cursor::new(s);

        let ss2: NestedSS2 = Snapshot::load(&mut cursor, l, vm2).unwrap();

        assert_eq!(ss2.a, 789);
        // So version 2 version map uses the default value of `d`
        assert_eq!(ss2.ss2.d, 0xabcd);
    }

    #[test]
    fn test_upgrade_hit_state_max_buffer_length() {
        let vm1 = VersionMap::new();
        let states_storage = Arc::new(MemoryStatesStorage::new(1024 * 1024 * 8));
        let mut upgrade_manager1 = UpgradeManager::new(vm1, states_storage.clone());

        let str1 = "A".repeat(STATE_MAX_BUF_LEN) + "abcde";
        let ss1 = SS1 {
            a: 678,
            b: str1.to_string(),
            c: 1.89,
        };

        let ss1_state_id = "ss";

        upgrade_manager1.save_state(ss1_state_id, &ss1).unwrap();
        upgrade_manager1.persist_states().unwrap();

        // After upgrading

        let vm2 = VersionMap::new();
        let m = UpgradeManager::new(vm2.clone(), states_storage);
        let upgrade_manager2 = m.fetch_states_and_restore().unwrap();

        let s = upgrade_manager2
            .restore_state(ss1_state_id)
            .unwrap()
            .unwrap();

        let l = s.len();

        let mut cursor = Cursor::new(s);

        let ss2: SS2 = Snapshot::load(&mut cursor, l, vm2).unwrap();

        assert_eq!(ss2.a, 678);
        assert_eq!(ss2.d, 0x1234);
        assert_eq!(ss2.b, str1);
    }
}
