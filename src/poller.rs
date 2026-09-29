//! Thin wrapper around a Linux `epoll` instance.
//!
//! Callers identify file descriptors by `u64` tokens and never see `nix`
//! types; all of `epoll` is contained in this module.

use nix::sys::epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags, EpollTimeout};
use std::{io, os::fd::AsFd};

/// An `epoll` instance paired with a fixed-size buffer that holds up to `N`
/// ready events per call to [`Poller::wait`].
pub(crate) struct Poller<const N: usize> {
    epoll: Epoll,
    events: [EpollEvent; N],
}

impl<const N: usize> Poller<N> {
    /// Creates a new `epoll` instance with an empty event buffer.
    pub(crate) fn build() -> io::Result<Self> {
        Ok(Self {
            epoll: Epoll::new(EpollCreateFlags::empty())?,
            events: [EpollEvent::empty(); N],
        })
    }

    /// Registers `fd` for readability under `token`.
    pub(crate) fn register(&self, fd: impl AsFd, token: u64) -> io::Result<()> {
        Ok(self
            .epoll
            .add(fd, EpollEvent::new(EpollFlags::EPOLLIN, token))?)
    }

    /// Unregisters `fd`.
    pub(crate) fn deregister(&self, fd: impl AsFd) -> io::Result<()> {
        Ok(self.epoll.delete(fd)?)
    }

    /// Blocks until at least one registered fd is ready and returns the
    /// tokens of the ready ones, at most `N` per call.
    pub(crate) fn wait(&mut self) -> io::Result<Vec<u64>> {
        let n = self.epoll.wait(&mut self.events, EpollTimeout::NONE)?;
        Ok(self.events[..n].iter().map(EpollEvent::data).collect())
    }
}
