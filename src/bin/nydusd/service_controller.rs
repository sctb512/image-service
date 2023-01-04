// Copyright (C) 2022 Alibaba Cloud. All rights reserved.
//
// SPDX-License-Identifier: (Apache-2.0 AND BSD-3-Clause)

use std::any::Any;
use std::fs::{metadata, File, OpenOptions};
use std::io::Result;
use std::os::unix::net::UnixStream;
#[cfg(target_os = "linux")]
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, MutexGuard};

use nydus_api::http::BlobCacheList;
use nydus_app::BuildTimeInfo;

use crate::blob_cache::BlobCacheMgr;
use crate::daemon::{
    DaemonError, DaemonResult, DaemonState, DaemonStateMachineContext, DaemonStateMachineInput,
    DaemonStateMachineSubscriber,
};
use crate::upgrade::{self, FscacheServiceState, UpgradeManager};
use crate::{FsService, NydusDaemon, SubCmdArgs, DAEMON_CONTROLLER};
#[cfg(target_os = "linux")]
use nydus::ensure_threads;

pub struct ServiceController {
    bti: BuildTimeInfo,
    id: Option<String>,
    request_sender: Arc<Mutex<Sender<DaemonStateMachineInput>>>,
    result_receiver: Mutex<Receiver<DaemonResult<()>>>,
    state: AtomicI32,
    supervisor: Option<String>,

    blob_cache_mgr: Arc<BlobCacheMgr>,
    upgrade_mgr: Option<Mutex<UpgradeManager>>,
    fscache_enabled: AtomicBool,
    #[cfg(target_os = "linux")]
    fscache: Mutex<Option<Arc<crate::fs_cache::FsCacheHandler>>>,
}

impl ServiceController {
    /// Start all enabled services.
    fn start_services(&self) -> Result<()> {
        info!("Starting all Nydus services...");

        #[cfg(target_os = "linux")]
        if self.fscache_enabled.load(Ordering::Acquire) {
            if let Some(fscache) = self.fscache.lock().unwrap().clone() {
                for _ in 0..fscache.working_threads() {
                    let fscache2 = fscache.clone();
                    std::thread::spawn(move || {
                        if let Err(e) = fscache2.run_loop() {
                            error!("Failed to run fscache service loop, {}", e);
                        }
                        // Notify the global service controller that one working thread is exiting.
                        if let Err(e) = crate::DAEMON_CONTROLLER.waker.wake() {
                            error!("Failed to notify the global service controller, {}", e);
                        }
                    });
                }
            }
        }

        Ok(())
    }

    /// Stop all enabled services.
    fn stop_services(&self) {
        info!("Stopping all Nydus services...");

        #[cfg(target_os = "linux")]
        if self.fscache_enabled.load(Ordering::Acquire) {
            if let Some(fscache) = self.fscache.lock().unwrap().take() {
                fscache.stop();
            }
        }
    }

    fn initialize_blob_cache(&self, config: &Option<serde_json::Value>) -> Result<()> {
        DAEMON_CONTROLLER.set_blob_cache_mgr(self.blob_cache_mgr.clone());

        // Create blob cache objects configured by the configuration file.
        if let Some(config) = config {
            if let Some(config1) = config.as_object() {
                if config1.contains_key("blobs") {
                    if let Ok(v) = serde_json::from_value::<BlobCacheList>(config.clone()) {
                        if let Err(e) = self.blob_cache_mgr.add_blob_list(&v) {
                            error!("Failed to add blob list: {}", e);
                            return Err(e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl ServiceController {
    pub fn initialize_fscache_service(
        &self,
        tag: Option<&str>,
        threads: usize,
        path: &str,
        file: Option<&File>,
    ) -> Result<()> {
        // Validate --fscache option value is an existing directory.
        let p = match Path::new(&path).canonicalize() {
            Err(e) => {
                error!("--fscache option needs a directory to cache files");
                return Err(e);
            }
            Ok(v) => {
                if !v.is_dir() {
                    error!("--fscache options needs a directory to cache files");
                    return Err(einval!("--fscache options is not a directory"));
                }
                v
            }
        };
        let p = match p.to_str() {
            Some(v) => v,
            None => {
                error!("--fscache option contains invalid characters");
                return Err(einval!("--fscache option contains invalid characters"));
            }
        };

        info!(
            "Create fscache instance at {} with tag {}, {} working threads",
            p,
            tag.unwrap_or("<none>"),
            threads
        );
        let fscache = crate::fs_cache::FsCacheHandler::new(
            "/dev/cachefiles",
            p,
            tag,
            self.blob_cache_mgr.clone(),
            threads,
            file,
        )?;
        *self.fscache.lock().unwrap() = Some(Arc::new(fscache));
        self.fscache_enabled.store(true, Ordering::Release);

        Ok(())
    }

    fn get_fscache_file(&self) -> Result<File> {
        if let Some(fscache) = self.fscache.lock().unwrap().clone() {
            let f = fscache.get_file().try_clone()?;
            Ok(f)
        } else {
            Err(einval!("fscache file not init"))
        }
    }
}

impl NydusDaemon for ServiceController {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn id(&self) -> Option<String> {
        self.id.clone()
    }

    fn get_state(&self) -> DaemonState {
        self.state.load(Ordering::Relaxed).into()
    }

    fn set_state(&self, state: DaemonState) {
        self.state.store(state as i32, Ordering::Relaxed);
    }

    fn version(&self) -> BuildTimeInfo {
        self.bti.clone()
    }

    fn start(&self) -> DaemonResult<()> {
        self.start_services()
            .map_err(|e| DaemonError::StartService(format!("{}", e)))
    }

    fn disconnect(&self) -> DaemonResult<()> {
        self.stop_services();
        Ok(())
    }

    fn wait(&self) -> DaemonResult<()> {
        Ok(())
    }

    fn supervisor(&self) -> Option<String> {
        self.supervisor.clone()
    }

    fn save(&self) -> DaemonResult<()> {
        upgrade::fscache_upgrade::save(self)
    }

    fn restore(&self) -> DaemonResult<()> {
        upgrade::fscache_upgrade::restore(self)
    }

    fn get_default_fs_service(&self) -> Option<Arc<dyn FsService>> {
        None
    }

    fn upgrade_mgr(&self) -> Option<MutexGuard<UpgradeManager>> {
        self.upgrade_mgr.as_ref().map(|mgr| mgr.lock().unwrap())
    }
}

impl DaemonStateMachineSubscriber for ServiceController {
    fn on_event(&self, event: DaemonStateMachineInput) -> DaemonResult<()> {
        self.request_sender
            .lock()
            .unwrap()
            .send(event)
            .map_err(|e| DaemonError::Channel(format!("send {:?}", e)))?;
        self.result_receiver
            .lock()
            .expect("Not expect poisoned lock!")
            .recv()
            .map_err(|e| DaemonError::Channel(format!("recv {:?}", e)))?
    }
}

fn is_sock_residual(sock: impl AsRef<Path>) -> bool {
    if metadata(&sock).is_ok() {
        return UnixStream::connect(&sock).is_err();
    }

    false
}
/// When nydusd starts, it checks that whether a previous nydusd died unexpected by:
///     1. Checking whether /dev/cachefiles can be opened.
///     2. Checking whether the API socket exists and the connection can established or not.
fn is_crashed(sock: &impl AsRef<Path>) -> Result<bool> {
    #[cfg(target_os = "linux")]
    if let Err(_e) = OpenOptions::new()
        .write(true)
        .read(true)
        .create(false)
        .open("/dev/cachefiles")
    {
        warn!("cachefiles devfd can not open, the devfd may hold by supervisor or another daemon.");
        if is_sock_residual(sock) {
            warn!("A previous daemon crashed! Try to failover later.");
            return Ok(true);
        }
        warn!("another daemon is running, will exit!");
        return Err(einval!("fscache stats error"));
    }
    Ok(false)
}

pub fn create_daemon(
    subargs: &SubCmdArgs,
    api_sock: Option<impl AsRef<Path>>,
    bti: BuildTimeInfo,
) -> Result<Arc<dyn NydusDaemon>> {
    let id = subargs.value_of("id").map(|id| id.to_string());
    let supervisor = subargs.value_of("supervisor").map(|s| s.to_string());
    let config = match subargs.value_of("config") {
        None => None,
        Some(path) => {
            let config = std::fs::read_to_string(path)?;
            let config: serde_json::Value = serde_json::from_str(&config)
                .map_err(|_e| einval!("invalid configuration file"))?;
            Some(config)
        }
    };

    let (to_sm, from_client) = channel::<DaemonStateMachineInput>();
    let (to_client, from_sm) = channel::<DaemonResult<()>>();
    let upgrade_mgr = supervisor
        .as_ref()
        .map(|s| Mutex::new(UpgradeManager::new(s.to_string().into())));
    let service_controller = ServiceController {
        bti,
        id,
        request_sender: Arc::new(Mutex::new(to_sm)),
        result_receiver: Mutex::new(from_sm),
        state: AtomicI32::new(DaemonState::INIT as i32),
        supervisor,

        blob_cache_mgr: Arc::new(BlobCacheMgr::new()),
        upgrade_mgr,
        fscache_enabled: AtomicBool::new(false),
        #[cfg(target_os = "linux")]
        fscache: Mutex::new(None),
    };

    service_controller.initialize_blob_cache(&config)?;
    let daemon = Arc::new(service_controller);
    let machine = DaemonStateMachineContext::new(daemon.clone(), from_client, to_client);
    machine.kick_state_machine()?;

    // Without api socket, nydusd can't do neither live-upgrade nor failover, so the helper
    // finding a victim is not necessary.
    if (api_sock.as_ref().is_some()
        && !subargs.is_present("upgrade")
        && !is_crashed(api_sock.as_ref().unwrap())?)
        || api_sock.is_none()
    {
        #[cfg(target_os = "linux")]
        if let Some(path) = subargs.value_of("fscache") {
            let tag = subargs.value_of("fscache-tag");
            let threads = if let Some(threads_value) = subargs.value_of("fscache-threads") {
                ensure_threads(threads_value).map_err(|err| einval!(err))?
            } else {
                1usize
            };
            daemon.initialize_fscache_service(tag, threads, path, None)?;
            let f = daemon.get_fscache_file()?;
            if let Some(mut mgr_guard) = daemon.upgrade_mgr() {
                mgr_guard.inner.lock().unwrap().hold_file(&f).map_err(|e| {
                    error!("Failed to hold fscache fd, {:?}", e);
                    eother!(e)
                })?;
                let state = FscacheServiceState {
                    threads,
                    path: path.to_string(),
                };
                upgrade::save_fscache_states(&mut mgr_guard, &state)?
            }
        }
        daemon
            .on_event(DaemonStateMachineInput::Mount)
            .map_err(|e| eother!(e))?;
        daemon
            .on_event(DaemonStateMachineInput::Start)
            .map_err(|e| eother!(e))?;
    }
    Ok(daemon)
}
