pub mod config;
pub mod protocol;
pub mod quic;

use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;

use quic::QuicBidiStream;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

pub enum FlezhaStream {
    Unix(UnixStream),
    Quic(QuicBidiStream),
}

impl FlezhaStream {
    pub fn try_clone(&self) -> io::Result<FlezhaStream> {
        match self {
            FlezhaStream::Unix(stream) => Ok(FlezhaStream::Unix(stream.try_clone()?)),
            FlezhaStream::Quic(stream) => Ok(FlezhaStream::Quic(stream.try_clone())),
        }
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        match self {
            FlezhaStream::Unix(stream) => stream.set_nonblocking(nonblocking),
            FlezhaStream::Quic(_) => Ok(()), // QUIC manages this internally via tokio
        }
    }

    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        match self {
            FlezhaStream::Unix(stream) => stream.shutdown(how),
            FlezhaStream::Quic(_) => Ok(()), // QUIC streams are closed when dropped
        }
    }

    #[cfg(unix)]
    pub fn as_raw_fd(&self) -> Option<i32> {
        match self {
            FlezhaStream::Unix(stream) => Some(stream.as_raw_fd()),
            FlezhaStream::Quic(_) => None,
        }
    }
}

impl Read for FlezhaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            FlezhaStream::Unix(stream) => stream.read(buf),
            FlezhaStream::Quic(stream) => stream.read(buf),
        }
    }
}

impl Write for FlezhaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            FlezhaStream::Unix(stream) => stream.write(buf),
            FlezhaStream::Quic(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            FlezhaStream::Unix(stream) => stream.flush(),
            FlezhaStream::Quic(stream) => stream.flush(),
        }
    }
}

// Implement Read/Write for &FlezhaStream (needed for Arc<FlezhaStream>)
impl Read for &FlezhaStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            FlezhaStream::Unix(stream) => (&*stream).read(buf),
            FlezhaStream::Quic(stream) => (&*stream).read(buf),
        }
    }
}

impl Write for &FlezhaStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            FlezhaStream::Unix(stream) => (&*stream).write(buf),
            FlezhaStream::Quic(stream) => (&*stream).write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            FlezhaStream::Unix(stream) => (&*stream).flush(),
            FlezhaStream::Quic(stream) => (&*stream).flush(),
        }
    }
}
