use std::io::Result;
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use sendfd::{RecvWithFd, SendWithFd};

pub trait StatesStorage: Sync + Send {
    fn write(&self, bytes: &[u8], fds: &[RawFd]) -> Result<()>;
    fn read(&self, buf: &mut [u8], fds: &mut [RawFd]) -> Result<(usize, usize)>;
}

pub struct SocketStatesStorage {
    socket: PathBuf,
}

impl SocketStatesStorage {
    pub fn new(s: PathBuf) -> Self {
        Self { socket: s }
    }
}

impl StatesStorage for SocketStatesStorage {
    fn write(&self, buf: &[u8], fds: &[RawFd]) -> Result<()> {
        let stream = UnixStream::connect(&self.socket)?;
        stream.send_with_fd(buf, fds)?;
        Ok(())
    }

    fn read(&self, buf: &mut [u8], fds: &mut [RawFd]) -> Result<(usize, usize)> {
        let stream = UnixStream::connect(&self.socket)?;
        let (len, fds_cnt) = stream.recv_with_fd(buf, fds)?;
        Ok((len, fds_cnt))
    }
}

#[cfg(test)]
pub mod mem_storage {
    use std::io::Result;
    use std::ops::Deref;
    use std::os::unix::io::RawFd;
    use std::sync::Mutex;

    use super::StatesStorage;
    pub struct MemoryStatesStorage {
        states: Mutex<(Vec<u8>, usize)>,
        cap: usize,
    }

    impl MemoryStatesStorage {
        pub fn new(cap: usize) -> Self {
            Self {
                states: Mutex::new((Vec::new(), 0)),
                cap,
            }
        }
    }

    impl StatesStorage for MemoryStatesStorage {
        fn write(&self, buf: &[u8], _fds: &[RawFd]) -> Result<()> {
            let mut guard = self.states.lock().unwrap();

            if guard.1 + buf.len() > self.cap {
                return Err(std::io::Error::new(std::io::ErrorKind::OutOfMemory, ""));
            }

            guard.0.extend_from_slice(buf);
            guard.1 += buf.len();

            Ok(())
        }

        fn read(&self, buf: &mut [u8], _fds: &mut [RawFd]) -> Result<(usize, usize)> {
            let guard = self.states.lock().unwrap();
            let pos = guard.deref().1;

            for (i, byte) in guard.deref().0.iter().take(pos).enumerate() {
                buf[i] = *byte;
            }

            Ok((pos, 0))
        }
    }
}
