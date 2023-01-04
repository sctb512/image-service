use std::convert::TryFrom;
use std::io::Cursor;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};

use nydus_api::http::BlobCacheEntry;
use snapshot::{Persist, Snapshot};
use versionize::{VersionMap, Versionize, VersionizeResult};
use versionize_derive::Versionize;

use fuse_backend_rs::api::{vfs::Vfs, VfsIndex};

use nydus::FsBackendType;

use crate::daemon::{DaemonError, DaemonResult};
use crate::fs_service::FsBackendUmountCmd;
use crate::FsBackendMountCmd;
use crate::DAEMON_CONTROLLER;
use upgrade::states_storage::SocketStatesStorage;
pub use upgrade::{UpgradeManager as BytedUpgradeManager, UpgradeManagerError as UpgradeMgrError};

const MOUNT_STATE_ID_PREFIX: &str = "MOUNT.";
const VFS_STATE_ID_PREFIX: &str = "VFS.";
const BLOB_STATE_ID_PREFIX: &str = "BLOB.";
const BLOB_STATE_ID_SLASH: &str = "/";
const FSCACHE_SERVICE_ID_PREFIX: &str = "FSCACHE.";

pub struct UpgradeManager {
    pub(crate) inner: Arc<Mutex<BytedUpgradeManager>>,
}

impl UpgradeManager {
    fn inner(&mut self) -> MutexGuard<BytedUpgradeManager> {
        // Not expect poisoned lock
        self.inner.lock().unwrap()
    }
}

pub struct VfsIndexedMounts {
    fs_index: VfsIndex,
    cmd: FsBackendMountState,
}

#[derive(Versionize)]
pub struct FscacheServiceState {
    pub threads: usize,
    pub path: String,
}

#[derive(Versionize)]
pub struct FsBackendMountState {
    fs_type: String,
    source: String,
    config: String,
    mountpoint: String,
}

impl Persist<'_> for FsBackendMountCmd {
    type State = FsBackendMountState;
    type ConstructorArgs = ();
    type Error = DaemonError;

    fn save(&self) -> Self::State {
        FsBackendMountState {
            fs_type: self.fs_type.to_string().to_lowercase(),
            source: self.source.clone(),
            config: self.config.clone(),
            mountpoint: self.mountpoint.clone(),
        }
    }

    fn restore(
        _constructor_args: Self::ConstructorArgs,
        state: &Self::State,
    ) -> std::result::Result<Self, Self::Error> {
        let r = Self {
            fs_type: FsBackendType::from_str(state.fs_type.as_str()).map_err(|e| {
                error!(
                    "Failed to restore mount command, fs_type {} , {}",
                    state.fs_type, e
                );
                DaemonError::UpgradeManager(UpgradeMgrError::Restore(
                    "restore mount command".to_string(),
                ))
            })?,
            source: state.source.clone(),
            config: state.config.clone(),
            mountpoint: state.mountpoint.clone(),
            // No need to perform files prefetch again.
            prefetch_files: None,
        };

        Ok(r)
    }
}

impl UpgradeManager {
    pub fn new(supervisor: PathBuf) -> Self {
        let s = SocketStatesStorage::new(supervisor);

        let vm = VersionMap::new();

        // Important. We must ensure that VersionMap root version is increased when nydusd or fuse-backend
        // changes its struct definition. And we have to public those structs from fuse-backend.

        info!("version map info: {:?}", vm);

        let inner = BytedUpgradeManager::new(vm, Arc::new(s));

        UpgradeManager {
            inner: Arc::new(Mutex::new(inner)),
        }
    }
}

#[derive(Versionize)]
struct MountStateWrapper {
    cmd: FsBackendMountState,
    vfs_index: u8,
}

#[derive(PartialEq)]
pub enum FailoverPolicy {
    Flush,
    Resend,
}

impl TryFrom<&str> for FailoverPolicy {
    type Error = std::io::Error;

    fn try_from(p: &str) -> std::result::Result<Self, Self::Error> {
        match p {
            "flush" => Ok(FailoverPolicy::Flush),
            "resend" => Ok(FailoverPolicy::Resend),
            x => Err(einval!(x)),
        }
    }
}

pub fn add_mounts_state(
    mgr: &mut UpgradeManager,
    cmd: FsBackendMountCmd,
    vfs_index: u8,
) -> DaemonResult<()> {
    let wrapper = MountStateWrapper {
        cmd: cmd.save(),
        vfs_index,
    };

    // Safe to unwrap since it is native string.
    let mut mount_state_id = String::from_str(MOUNT_STATE_ID_PREFIX).unwrap();
    mount_state_id.push_str(&cmd.mountpoint);

    // Not expected poisoned lock here.
    mgr.inner()
        .save_state(&mount_state_id, &wrapper)
        .map_err(DaemonError::UpgradeManager)
}

pub fn update_mounts_state(mgr: &mut UpgradeManager, cmd: FsBackendMountCmd) -> DaemonResult<()> {
    // Safe to unwrap since it is native string.
    let mut mount_state_id = String::from_str(MOUNT_STATE_ID_PREFIX).unwrap();
    mount_state_id.push_str(&cmd.mountpoint);

    // update method does not provide vfs index. So the state must be found.
    let old_state = mgr
        .inner()
        .restore_state(&mount_state_id)
        .map_err(|e| {
            error!("Restore state error");
            DaemonError::UpgradeManager(e)
        })?
        .ok_or(DaemonError::NotFound)?;

    let old_state_len = old_state.len();
    let mut cursor = Cursor::new(old_state);
    let vm = mgr.inner().get_version_map();

    let old_wrapper: MountStateWrapper = Snapshot::load(&mut cursor, old_state_len, vm)
        .map_err(|e| {
            error!("Failed to restore a single mount state, {:?}", e);
            UpgradeMgrError::Snapshot(e)
        })
        .map_err(DaemonError::UpgradeManager)?;

    let new_wrapper = MountStateWrapper {
        vfs_index: old_wrapper.vfs_index,
        cmd: cmd.save(),
    };

    // Not expected poisoned lock here.
    mgr.inner()
        .save_state(&mount_state_id, &new_wrapper)
        .map_err(DaemonError::UpgradeManager)?;

    Ok(())
}

pub fn remove_mounts_state(mgr: &mut UpgradeManager, cmd: FsBackendUmountCmd) -> DaemonResult<()> {
    let mut mount_state_id = MOUNT_STATE_ID_PREFIX.to_string();
    mount_state_id.push_str(&cmd.mountpoint);
    // Not expected poisoned lock here.
    mgr.inner().deref_mut().remove_state(&mount_state_id);

    Ok(())
}

pub fn save_vfs_states(mgr: &mut UpgradeManager, vfs: &Vfs) -> DaemonResult<()> {
    // Safe to unwrap since it is native UTF-8 string.
    let mut state_id = String::from_str(VFS_STATE_ID_PREFIX).unwrap();
    state_id.push_str("vfs");
    let vfs_states = vfs.save();
    // Not expected poisoned lock here.
    mgr.inner()
        .save_state(&state_id, &vfs_states)
        .map_err(DaemonError::UpgradeManager)
}

pub fn add_blob_entry_state(mgr: &mut UpgradeManager, entry: &BlobCacheEntry) -> DaemonResult<()> {
    let mut blob_state_id = BLOB_STATE_ID_PREFIX.to_string();
    blob_state_id.push_str(&entry.domain_id);
    blob_state_id.push_str(BLOB_STATE_ID_SLASH);
    blob_state_id.push_str(&entry.blob_id);
    let blob_state = entry.save();

    mgr.inner
        .lock()
        .unwrap()
        .deref_mut()
        .save_state(&blob_state_id, &blob_state)
        .map_err(DaemonError::UpgradeManager)
}

pub fn remove_blob_entry_state(
    mgr: &mut UpgradeManager,
    domain_id: &str,
    blob_id: &str,
) -> DaemonResult<()> {
    let mut blob_state_id = BLOB_STATE_ID_PREFIX.to_string();
    blob_state_id.push_str(domain_id);
    blob_state_id.push_str(BLOB_STATE_ID_SLASH);
    // for no shared domain mode, snapshotter will call unbind without blob_id
    if !blob_id.is_empty() {
        blob_state_id.push_str(blob_id);
    } else {
        blob_state_id.push_str(domain_id);
    }

    mgr.inner
        .lock()
        .unwrap()
        .deref_mut()
        .remove_state(&blob_state_id);

    Ok(())
}

pub fn save_fscache_states(
    mgr: &mut UpgradeManager,
    stat: &FscacheServiceState,
) -> DaemonResult<()> {
    let fscache_service_id = FSCACHE_SERVICE_ID_PREFIX.to_string();

    mgr.inner
        .lock()
        .unwrap()
        .deref_mut()
        .save_state(&fscache_service_id, stat)
        .map_err(DaemonError::UpgradeManager)
}

#[cfg(target_os = "linux")]
pub mod fscache_upgrade {
    use snapshot::Snapshot;
    use std::io::Cursor;
    use std::ops::DerefMut;
    use upgrade::UpgradeManagerError;

    use crate::daemon::{DaemonError, DaemonResult, NydusDaemon};
    use crate::service_controller::ServiceController;

    use super::*;

    pub fn save(daemon: &ServiceController) -> DaemonResult<()> {
        if let Some(mut mgr) = daemon.upgrade_mgr() {
            let um = mgr.deref_mut().inner.lock().unwrap();
            um.persist_states().unwrap();
        }
        Ok(())
    }

    pub fn restore(daemon: &ServiceController) -> DaemonResult<()> {
        if let Some(mut um) = daemon.upgrade_mgr() {
            let mut mgr = um.deref_mut().inner.lock().unwrap();
            let restored_um = mgr.fetch_states_and_restore().map_err(|e| {
                error!("Failed to restore upgrade manager, {:?}", e);
                DaemonError::UpgradeManager(e)
            })?;

            // Upgrade manager must be created during nydusd startup. Mainly because it must receive
            // supervisor socket path and connect to external supervisor to fetch stored states back.
            // As upgrade manager also act as a snapshot, recover it by swapping its states.
            // Swap recovered states
            mgr.swap_states(restored_um);

            let mut fscache_state: Option<FscacheServiceState> = None;

            if let Some(blob_mgr) = DAEMON_CONTROLLER.get_blob_cache_mgr() {
                let states_ids = mgr.list_all_states_ids();
                for id in &states_ids {
                    let s = mgr
                        .restore_state(id)
                        .map_err(|e| {
                            error!("Restore a single state error");
                            DaemonError::UpgradeManager(e)
                        })?
                        .ok_or(DaemonError::NotFound)?;
                    let vm = mgr.get_version_map();
                    let len = s.len();
                    let mut cursor = Cursor::new(s);

                    if id.starts_with(BLOB_STATE_ID_PREFIX) {
                        debug!("Restoring blob cache entry {}", id);
                        let entry_stat = Snapshot::load(&mut cursor, len, vm)
                            .map_err(|e| {
                                error!("Failed to restore a single mount state, {:?}", e);
                                UpgradeManagerError::Snapshot(e)
                            })
                            .map_err(DaemonError::UpgradeManager)?;

                        let entry = BlobCacheEntry::restore((), &entry_stat).map_err(|e| {
                            error!("blob entry restore failed, {:?}", e);
                            DaemonError::Unsupported
                        })?;

                        blob_mgr.add_blob_entry(&entry).map_err(|e| {
                            error!("add blob entry failed, {:?}", e);
                            DaemonError::Unsupported
                        })?;
                    } else if id.starts_with(FSCACHE_SERVICE_ID_PREFIX) {
                        fscache_state = Some(
                            Snapshot::load(&mut cursor, len, vm)
                                .map_err(|e| {
                                    error!("Failed to restore a single mount state, {:?}", e);
                                    UpgradeManagerError::Snapshot(e)
                                })
                                .map_err(DaemonError::UpgradeManager)?,
                        )
                    } else {
                        warn!("Unknown state prefix {}", id);
                    }
                }

                //init fscache handler with fd restored
                if let Some(s) = fscache_state {
                    if let Some(f) = mgr.return_file() {
                        daemon
                            .initialize_fscache_service(None, s.threads, &s.path, Some(&f))
                            .map_err(|e| {
                                error!("initialize fscache service failed, {:?}", e);
                                DaemonError::Unsupported
                            })?;
                    } else {
                        error!("Not found fscache service states!");
                        return Err(DaemonError::Unsupported);
                    }
                } else {
                    error!("Not found fscache service states!");
                    return Err(DaemonError::Unsupported);
                }

                return Ok(());
            }
        }
        Err(DaemonError::Unsupported)
    }
}

pub mod fusedev_upgrade {
    use std::io::Cursor;
    use std::ops::DerefMut;

    use fuse_backend_rs::api::vfs::upgrade::VfsState;

    use snapshot::Snapshot;
    use upgrade::UpgradeManagerError;

    use crate::daemon::{DaemonError, DaemonResult, NydusDaemon};
    use crate::fs_service::fs_backend_factory;
    use crate::fusedev::FusedevDaemon;

    use super::*;

    pub fn save(daemon: &FusedevDaemon) -> DaemonResult<()> {
        if let Some(service) = daemon.get_default_fs_service() {
            if let Some(mut mgr) = service.upgrade_mgr() {
                // Not expect poisoned lock
                let um = mgr.deref_mut().inner.lock().unwrap();
                um.persist_states().map_err(DaemonError::UpgradeManager)?
            }
        }

        Ok(())
    }

    pub fn restore(daemon: &FusedevDaemon) -> DaemonResult<()> {
        if let Some(service) = daemon.get_default_fs_service() {
            if let Some(mut um) = service.upgrade_mgr() {
                // Not expect poisoned lock.
                let mut mgr = um.deref_mut().inner.lock().unwrap();

                let restored_um = mgr.fetch_states_and_restore().map_err(|e| {
                    error!("Failed to restore upgrade manager, {:?}", e);
                    DaemonError::UpgradeManager(e)
                })?;

                // Upgrade manager must be created during nydusd startup. Mainly because it must receive
                // supervisor socket path and connect to external supervisor to fetch stored states back.
                // As upgrade manager also act as a snapshot, recover it by swapping its states.
                // Swap recovered states
                mgr.swap_states(restored_um);

                let vfs = service.get_vfs();

                let mut vfs_state: Option<VfsState> = None;

                let states_ids = mgr.list_all_states_ids();

                for id in &states_ids {
                    let s = mgr
                        .restore_state(id)
                        .map_err(|e| {
                            error!("Restore a single state error");
                            DaemonError::UpgradeManager(e)
                        })?
                        .ok_or(DaemonError::NotFound)?;
                    let vm = mgr.get_version_map();
                    let len = s.len();
                    let mut cursor = Cursor::new(s);

                    if id.starts_with(MOUNT_STATE_ID_PREFIX) {
                        warn!("Restoring mountpoint {}", id);
                        let wrapper: MountStateWrapper = Snapshot::load(&mut cursor, len, vm)
                            .map_err(|e| {
                                error!("Failed to restore a single mount state, {:?}", e);
                                UpgradeManagerError::Snapshot(e)
                            })
                            .map_err(DaemonError::UpgradeManager)?;

                        let cmd = FsBackendMountCmd::restore((), &wrapper.cmd)?;
                        let vfs_index = wrapper.vfs_index;

                        let backend = fs_backend_factory(&cmd)?;

                        vfs.indexed_mount(backend, vfs_index, &cmd.mountpoint)
                            .map_err(DaemonError::Vfs)?;
                    } else if id.starts_with(VFS_STATE_ID_PREFIX) {
                        vfs_state = Some(
                            Snapshot::load(&mut cursor, len, vm)
                                .map_err(|e| {
                                    error!("Failed to restore a single mount state, {:?}", e);
                                    UpgradeManagerError::Snapshot(e)
                                })
                                .map_err(DaemonError::UpgradeManager)?,
                        );
                    } else {
                        warn!("Unknown state prefix {}", id);
                    }
                }

                // Finally, all fs backends' root entries are gathered,
                // let's start to restore vfs' states
                if let Some(ref s) = vfs_state {
                    let restored_vfs = Vfs::restore((), s).map_err(|e| {
                        error!("Restore vfs states, {:?}", e);
                        DaemonError::UpgradeManager(UpgradeMgrError::Restore(format!("{:?}", e)))
                    })?;
                    vfs.swap_states(restored_vfs)
                } else {
                    warn!("No VFS states!");
                }

                if let Some(f) = mgr.return_file() {
                    daemon.restore_session(f)
                }

                return Ok(());
            }
        }

        Err(DaemonError::Unsupported)
    }
}
