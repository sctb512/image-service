// Copyright 2020 Ant Group. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

//! Help library to manage network connections.
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Result};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use log::{max_level, Level};

use reqwest::header::{HeaderName, HeaderValue};
use reqwest::{
    self,
    blocking::{Body, Client, Response},
    header::HeaderMap,
    redirect::Policy,
    Method, StatusCode, Url,
};

use nydus_api::http::{MirrorConfig, OssConfig, ProxyConfig, RegistryConfig};
use url::ParseError;

const HEADER_AUTHORIZATION: &str = "Authorization";

const RATE_LIMITED_LOG_TIME: u8 = 2;

thread_local! {
    pub static LAST_FALLBACK_AT: RefCell<SystemTime> = RefCell::new(UNIX_EPOCH);
}

/// Error codes related to network communication.
#[derive(Debug)]
pub enum ConnectionError {
    Disconnected,
    ErrorWithMsg(String),
    Common(reqwest::Error),
    Format(reqwest::Error),
    Url(ParseError),
    Scheme,
    Host,
    Port,
}

/// Specialized `Result` for network communication.
type ConnectionResult<T> = std::result::Result<T, ConnectionError>;

/// Generic configuration for storage backends.
#[derive(Debug, Clone)]
pub(crate) struct ConnectionConfig {
    pub proxy: ProxyConfig,
    pub mirrors: Vec<MirrorConfig>,
    pub skip_verify: bool,
    pub timeout: u32,
    pub connect_timeout: u32,
    pub retry_limit: u8,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            proxy: ProxyConfig::default(),
            mirrors: Vec::<MirrorConfig>::new(),
            skip_verify: false,
            timeout: 5,
            connect_timeout: 5,
            retry_limit: 0,
        }
    }
}

impl From<OssConfig> for ConnectionConfig {
    fn from(c: OssConfig) -> ConnectionConfig {
        ConnectionConfig {
            proxy: c.proxy,
            mirrors: c.mirrors,
            skip_verify: c.skip_verify,
            timeout: c.timeout,
            connect_timeout: c.connect_timeout,
            retry_limit: c.retry_limit,
        }
    }
}

impl From<RegistryConfig> for ConnectionConfig {
    fn from(c: RegistryConfig) -> ConnectionConfig {
        ConnectionConfig {
            proxy: c.proxy,
            mirrors: c.mirrors,
            skip_verify: c.skip_verify,
            timeout: c.timeout,
            connect_timeout: c.connect_timeout,
            retry_limit: c.retry_limit,
        }
    }
}

/// HTTP request data with progress callback.
#[derive(Clone)]
pub struct Progress<R> {
    inner: R,
    current: usize,
    total: usize,
    callback: fn((usize, usize)),
}

impl<R> Progress<R> {
    /// Create a new `Progress` object.
    pub fn new(r: R, total: usize, callback: fn((usize, usize))) -> Progress<R> {
        Progress {
            inner: r,
            current: 0,
            total,
            callback,
        }
    }
}

impl<R: Read + Send + 'static> Read for Progress<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf).map(|count| {
            self.current += count as usize;
            (self.callback)((self.current, self.total));
            count
        })
    }
}

/// HTTP request data to send to server.
#[derive(Clone)]
pub enum ReqBody<R> {
    Read(Progress<R>, usize),
    Buf(Vec<u8>),
    Form(HashMap<String, String>),
}

#[derive(Debug)]
struct ProxyHealth {
    status: AtomicBool,
    ping_url: Option<Url>,
    check_interval: Duration,
}

impl ProxyHealth {
    fn new(check_interval: u64, ping_url: Option<Url>) -> Self {
        ProxyHealth {
            status: AtomicBool::from(true),
            ping_url,
            check_interval: Duration::from_secs(check_interval),
        }
    }

    fn ok(&self) -> bool {
        self.status.load(Ordering::Relaxed)
    }

    fn set(&self, health: bool) {
        self.status.store(health, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct Proxy {
    client: Client,
    health: ProxyHealth,
    fallback: bool,
}

/// Check whether the HTTP status code is a success result.
pub(crate) fn is_success_status(status: StatusCode) -> bool {
    status >= StatusCode::OK && status < StatusCode::BAD_REQUEST
}

/// Convert a HTTP `Response` into an `Result<Response>`.
pub(crate) fn respond(resp: Response, catch_status: bool) -> ConnectionResult<Response> {
    if !catch_status || is_success_status(resp.status()) {
        Ok(resp)
    } else {
        let msg = resp.text().map_err(ConnectionError::Format)?;
        Err(ConnectionError::ErrorWithMsg(msg))
    }
}

/// A network connection to communicate with remote server.
#[derive(Debug)]
pub(crate) struct Connection {
    client: Client,
    proxy: Option<Proxy>,
    mirror_state: MirrorState,
    shutdown: AtomicBool,
}

#[derive(Debug)]
pub(crate) struct MirrorState {
    mirrors: Vec<Arc<Mirror>>,
    /// Current mirror, None if there is no mirror available.
    current: ArcSwapOption<Mirror>,
}

#[derive(Debug)]
pub(crate) struct Mirror {
    /// Information for mirror from configuration file.
    config: MirrorConfig,
    /// Mirror status, it will be set to false by atomic operation when mirror is not work.
    status: AtomicBool,
    /// Falied times for mirror, the status will be marked as false when failed_times = failed_limit.
    failed_times: AtomicU8,
    /// Failed limit for mirror.
    failed_limit: u8,
}

impl Connection {
    /// Create a new connection according to the configuration.
    pub fn new(config: &ConnectionConfig) -> Result<Arc<Connection>> {
        info!("backend config: {:?}", config);
        let client = Self::build_connection("", config)?;

        let proxy = if !config.proxy.url.is_empty() {
            let ping_url = if !config.proxy.ping_url.is_empty() {
                Some(Url::from_str(&config.proxy.ping_url).map_err(|e| einval!(e))?)
            } else {
                None
            };
            Some(Proxy {
                client: Self::build_connection(&config.proxy.url, config)?,
                health: ProxyHealth::new(config.proxy.check_interval, ping_url),
                fallback: config.proxy.fallback,
            })
        } else {
            None
        };

        let mut mirrors = Vec::new();
        for mirror_config in config.mirrors.iter() {
            if !mirror_config.host.is_empty() {
                mirrors.push(Arc::new(Mirror {
                    config: mirror_config.clone(),
                    status: AtomicBool::from(true),
                    // Maybe read from configuration file
                    failed_limit: 5,
                    failed_times: AtomicU8::from(0),
                }));
            }
        }

        let current = if let Some(first_mirror) = mirrors.first() {
            ArcSwapOption::from(Some(first_mirror.clone()))
        } else {
            ArcSwapOption::from(None)
        };

        let connection = Arc::new(Connection {
            client,
            proxy,
            mirror_state: MirrorState { mirrors, current },
            shutdown: AtomicBool::new(false),
        });

        if let Some(proxy) = &connection.proxy {
            if proxy.health.ping_url.is_some() {
                let conn = connection.clone();
                let connect_timeout = config.connect_timeout;

                // Spawn thread to update the health status of proxy server
                thread::spawn(move || {
                    let proxy = conn.proxy.as_ref().unwrap();
                    let ping_url = proxy.health.ping_url.as_ref().unwrap();
                    let mut last_success = true;

                    loop {
                        let client = Client::new();
                        let _ = client
                            .get(ping_url.clone())
                            .timeout(Duration::from_secs(connect_timeout as u64))
                            .send()
                            .map(|resp| {
                                let success = is_success_status(resp.status());
                                if last_success && !success {
                                    warn!(
                                        "Detected proxy unhealthy when pinging proxy, response status {}",
                                        resp.status()
                                    );
                                } else if !last_success && success {
                                    info!("Backend proxy recovered")
                                }
                                last_success = success;
                                proxy.health.set(success);
                            })
                            .map_err(|e| {
                                if last_success {
                                    warn!("Detected proxy unhealthy when ping proxy, {}", e);
                                }
                                last_success = false;
                                proxy.health.set(false)
                            });

                        if conn.shutdown.load(Ordering::Acquire) {
                            break;
                        }
                        thread::sleep(proxy.health.check_interval);
                        if conn.shutdown.load(Ordering::Acquire) {
                            break;
                        }
                    }
                });
            }
        }
        // TODO: check mirrors' health

        Ok(connection)
    }

    /// Shutdown the connection.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Send a request to server and wait for response.
    pub fn call<R: Read + Send + 'static>(
        &self,
        method: Method,
        url: &str,
        query: Option<&[(&str, &str)]>,
        data: Option<ReqBody<R>>,
        headers: &mut HeaderMap,
        catch_status: bool,
    ) -> ConnectionResult<Response> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(ConnectionError::Disconnected);
        }

        if let Some(proxy) = &self.proxy {
            if proxy.health.ok() {
                let data_cloned: Option<ReqBody<R>> = match data.as_ref() {
                    Some(ReqBody::Form(form)) => Some(ReqBody::Form(form.clone())),
                    Some(ReqBody::Buf(buf)) => Some(ReqBody::Buf(buf.clone())),
                    _ => None,
                };
                let result = self.call_inner(
                    &proxy.client,
                    method.clone(),
                    url,
                    &query,
                    data_cloned,
                    headers,
                    catch_status,
                    true,
                );

                match result {
                    Ok(resp) => {
                        if !proxy.fallback || resp.status() < StatusCode::INTERNAL_SERVER_ERROR {
                            return Ok(resp);
                        }
                    }
                    Err(err) => {
                        if !proxy.fallback {
                            return Err(err);
                        }
                    }
                }
                // If proxy server responds invalid status code or http connection failed, we need to
                // fallback to origin server, the policy only applicable to non-upload operation
                warn!("Request proxy server failed, fallback to original server");
            } else {
                LAST_FALLBACK_AT.with(|f| {
                    let current = SystemTime::now();
                    if current.duration_since(*f.borrow()).unwrap().as_secs()
                        >= RATE_LIMITED_LOG_TIME as u64
                    {
                        warn!("Proxy server is not healthy, fallback to original server");
                        f.replace(current);
                    }
                })
            }
        }

        let current_mirror = self.mirror_state.current.load();

        if let Some(mirror) = current_mirror.as_ref() {
            let data_cloned: Option<ReqBody<R>> = match data.as_ref() {
                Some(ReqBody::Form(form)) => Some(ReqBody::Form(form.clone())),
                Some(ReqBody::Buf(buf)) => Some(ReqBody::Buf(buf.clone())),
                _ => None,
            };

            let mirror_host = Url::parse(&mirror.config.host).map_err(ConnectionError::Url)?;

            let mut current_url = Url::parse(url).map_err(ConnectionError::Url)?;

            current_url
                .set_scheme(mirror_host.scheme())
                .map_err(|_| ConnectionError::Scheme)?;
            current_url
                .set_host(mirror_host.host_str())
                .map_err(|_| ConnectionError::Host)?;
            current_url
                .set_port(mirror_host.port())
                .map_err(|_| ConnectionError::Port)?;

            for (key, value) in mirror.config.headers.iter() {
                headers.insert(
                    HeaderName::from_str(key).unwrap(),
                    HeaderValue::from_str(value).unwrap(),
                );
            }

            warn!("mirror call_inner headers: {:?}", headers);

            let result = self.call_inner(
                &self.client,
                method.clone(),
                current_url.to_string().as_str(),
                &query,
                data_cloned,
                headers,
                catch_status,
                false,
            );

            match result {
                Ok(resp) => {
                    warn!("mirror call_inner ok resp.status(): {:?}", resp.status());
                    if resp.status() < StatusCode::INTERNAL_SERVER_ERROR {
                        return Ok(resp);
                    }
                }
                Err(err) => {
                    warn!(
                        "request mirror server failed, mirror: {:?},  error: {:?}",
                        mirror, err
                    );
                    mirror.failed_times.fetch_add(1, Ordering::Relaxed);

                    if mirror.failed_times.load(Ordering::Relaxed) >= mirror.failed_limit {
                        warn!(
                            "reach to fail limit {}, disable mirror: {:?}",
                            mirror.failed_limit, mirror
                        );
                        mirror.status.store(false, Ordering::Relaxed);

                        let mut idx = 0;
                        loop {
                            if idx == self.mirror_state.mirrors.len() {
                                break None;
                            }
                            let m = &self.mirror_state.mirrors[idx];
                            if m.status.load(Ordering::Relaxed) {
                                warn!("mirror server has been changed to {:?}", m);
                                break Some(m);
                            }

                            idx += 1;
                        }
                        .map(|m| self.mirror_state.current.store(Some(m.clone())))
                        .unwrap_or_else(|| self.mirror_state.current.store(None));
                    }
                }
            }
            warn!("Failed to request mirror server, fallback to original server.");
        }

        self.call_inner(
            &self.client,
            method,
            url,
            &query,
            data,
            headers,
            catch_status,
            false,
        )
    }

    fn build_connection(proxy: &str, config: &ConnectionConfig) -> Result<Client> {
        let connect_timeout = if config.connect_timeout != 0 {
            Some(Duration::from_secs(config.connect_timeout as u64))
        } else {
            None
        };
        let timeout = if config.timeout != 0 {
            Some(Duration::from_secs(config.timeout as u64))
        } else {
            None
        };

        let mut cb = Client::builder()
            .timeout(timeout)
            .connect_timeout(connect_timeout)
            .redirect(Policy::none());

        if config.skip_verify {
            cb = cb.danger_accept_invalid_certs(true);
        }

        if !proxy.is_empty() {
            cb = cb.proxy(reqwest::Proxy::all(proxy).map_err(|e| einval!(e))?)
        }

        cb.build().map_err(|e| einval!(e))
    }

    #[allow(clippy::too_many_arguments)]
    fn call_inner<R: Read + Send + 'static>(
        &self,
        client: &Client,
        method: Method,
        url: &str,
        query: &Option<&[(&str, &str)]>,
        data: Option<ReqBody<R>>,
        headers: &HeaderMap,
        catch_status: bool,
        proxy: bool,
    ) -> ConnectionResult<Response> {
        // Only clone header when debugging to reduce potential overhead.
        let display_headers = if max_level() >= Level::Debug {
            let mut display_headers = headers.clone();
            display_headers.remove(HEADER_AUTHORIZATION);
            Some(display_headers)
        } else {
            None
        };
        let has_data = data.is_some();
        let start = Instant::now();

        warn!("request headers: {:?}", headers.clone());

        let mut rb = client.request(method.clone(), url).headers(headers.clone());
        if let Some(q) = query.as_ref() {
            rb = rb.query(q);
        }

        warn!("request rb: {:?}", rb);

        let ret;
        if let Some(data) = data {
            warn!("have data");
            match data {
                ReqBody::Read(body, total) => {
                    let body = Body::sized(body, total as u64);
                    ret = rb.body(body).send();
                }
                ReqBody::Buf(buf) => {
                    ret = rb.body(buf).send();
                }
                ReqBody::Form(form) => {
                    ret = rb.form(&form).send();
                }
            }
        } else {
            warn!("no data");
            ret = rb.body("").send();
        }

        debug!(
            "{} Request: {} {} headers: {:?}, proxy: {}, data: {}, duration: {}ms",
            std::thread::current().name().unwrap_or_default(),
            method,
            url,
            display_headers,
            proxy,
            has_data,
            Instant::now().duration_since(start).as_millis(),
        );

        match ret {
            Err(err) => Err(ConnectionError::Common(err)),
            Ok(resp) => respond(resp, catch_status),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_progress() {
        let buf = vec![0x1u8, 2, 3, 4, 5];
        let mut progress = Progress::new(Cursor::new(buf), 5, |(curr, total)| {
            assert!(curr == 2 || curr == 4);
            assert_eq!(total, 5);
        });

        let mut buf1 = [0x0u8; 2];
        assert_eq!(progress.read(&mut buf1).unwrap(), 2);
        assert_eq!(buf1[0], 1);
        assert_eq!(buf1[1], 2);

        assert_eq!(progress.read(&mut buf1).unwrap(), 2);
        assert_eq!(buf1[0], 3);
        assert_eq!(buf1[1], 4);
    }

    #[test]
    fn test_proxy_health() {
        let checker = ProxyHealth::new(5, None);

        assert!(checker.ok());
        assert!(checker.ok());
        checker.set(false);
        assert!(!checker.ok());
        assert!(!checker.ok());
        checker.set(true);
        assert!(checker.ok());
        assert!(checker.ok());
    }

    #[test]
    fn test_is_success_status() {
        assert!(!is_success_status(StatusCode::CONTINUE));
        assert!(is_success_status(StatusCode::OK));
        assert!(is_success_status(StatusCode::PERMANENT_REDIRECT));
        assert!(!is_success_status(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn test_connection_config_default() {
        let config = ConnectionConfig::default();

        assert_eq!(config.timeout, 5);
        assert_eq!(config.connect_timeout, 5);
        assert_eq!(config.retry_limit, 0);
        assert_eq!(config.proxy.check_interval, 5);
        assert!(config.proxy.fallback);
        assert_eq!(config.proxy.ping_url, "");
        assert_eq!(config.proxy.url, "");
        assert!(config.mirrors.is_empty());
    }
}
