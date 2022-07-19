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
