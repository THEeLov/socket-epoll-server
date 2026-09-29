//! Unix socket server that multiplexes client connections with `epoll`.

use crate::{connection::Connection, poller::Poller};
use std::{
    collections::HashMap,
    fs, io,
    os::{fd::AsRawFd, unix::net::UnixListener},
    path::{Path, PathBuf},
};
use tracing::{info, trace, warn};

const EPOLL_BUFFER: usize = 1024;
const LISTENER_TOKEN: u64 = 0;

/// Application logic plugged into a [`Server`].
///
/// The server owns the sockets and buffers; the handler decides what the
/// received bytes mean and what to send back.
pub trait Handler {
    /// Per-connection data the application wants to keep.
    type State: Default;

    /// Called once after a client connects.
    fn on_connect(&mut self, _conn: &mut Connection<Self::State>) {}

    /// Called with all bytes received from `conn` and not yet consumed.
    ///
    /// Returns how many bytes of `input` were consumed. The rest is kept and
    /// passed again, together with new data, on the next call. Return `0` if
    /// `input` does not yet contain a complete message.
    fn on_data(&mut self, conn: &mut Connection<Self::State>, input: &[u8]) -> usize;

    /// Called once before a connection is dropped.
    fn on_close(&mut self, _conn: &mut Connection<Self::State>) {}
}

/// An `epoll` event loop serving clients on a Unix socket.
pub struct Server<H: Handler> {
    path: PathBuf,
    listener: UnixListener,
    poller: Poller<EPOLL_BUFFER>,
    clients: HashMap<u64, Connection<H::State>>,
    handler: H,
}

impl<H: Handler> Server<H> {
    /// Creates a server listening on the Unix socket at `path`, removing
    /// any file left there by a previous run.
    ///
    /// # Errors
    ///
    /// Returns an error if an existing file at `path` cannot be removed, the
    /// socket cannot be bound or made non-blocking, or `epoll` setup fails.
    pub fn bind(path: impl AsRef<Path>, handler: H) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        let listener = UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;

        let poller = Poller::build()?;
        poller.register(&listener, LISTENER_TOKEN)?;

        info!(path = %path.display(), "listening");
        Ok(Self {
            path,
            listener,
            poller,
            clients: HashMap::new(),
            handler,
        })
    }

    /// Runs the event loop. Only returns if waiting for events fails;
    /// failures of individual clients are logged and do not stop the loop.
    ///
    /// # Errors
    ///
    /// Returns an error if waiting on `epoll` fails.
    pub fn run(&mut self) -> io::Result<()> {
        loop {
            trace!("waiting for events");
            let tokens = self.poller.wait()?;
            trace!(n = tokens.len(), "got events");

            for token in tokens {
                if token == LISTENER_TOKEN {
                    self.accept_client();
                } else {
                    self.handle_client(token);
                }
            }
        }
    }

    /// Reads from a ready client, lets the handler process the input, and
    /// flushes any reply.
    fn handle_client(&mut self, token: u64) {
        // `self.clients` and `self.handler` are different fields, so they
        // can be borrowed mutably at the same time.
        let Some(conn) = self.clients.get_mut(&token) else {
            return; // stale event for a client removed earlier in this batch
        };

        match conn.read_available() {
            Ok(0) => conn.close(), // client hung up
            Ok(_) => {
                let handler = &mut self.handler;
                conn.process_input(|conn, input| handler.on_data(conn, input));
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => {
                warn!(fd = token, error = %e, "read failed");
                conn.close();
            }
        }

        if let Err(e) = conn.flush() {
            warn!(fd = token, error = %e, "write failed");
            conn.close();
        }

        if conn.is_closed() {
            self.remove_client(token);
        }
    }

    /// Accepts a pending connection, registers it and tells the handler.
    /// Any failure is logged and the connection is dropped.
    fn accept_client(&mut self) {
        let stream = match self.listener.accept() {
            Ok((stream, _)) => stream,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return,
            Err(e) => {
                warn!(error = %e, "failed to accept client");
                return;
            }
        };

        let token = fd_token(&stream);

        if let Err(e) = stream.set_nonblocking(true) {
            warn!(fd = token, error = %e, "failed to set client non-blocking");
            return;
        }
        if let Err(e) = self.poller.register(&stream, token) {
            warn!(fd = token, error = %e, "failed to register client with epoll");
            return;
        }

        let mut conn = Connection::new(token, stream, H::State::default());
        self.handler.on_connect(&mut conn);
        let closed = conn.is_closed();
        self.clients.insert(token, conn);
        info!(fd = token, "accepted client");

        if closed {
            self.remove_client(token);
        }
    }

    /// Tells the handler, deregisters the client and drops it, which closes
    /// its socket.
    fn remove_client(&mut self, token: u64) {
        let Some(mut conn) = self.clients.remove(&token) else {
            return;
        };
        self.handler.on_close(&mut conn);
        let _ = conn.flush(); // best effort: send any last reply
        if let Err(e) = self.poller.deregister(conn.stream()) {
            warn!(fd = token, error = %e, "failed to deregister client");
        }
        info!(fd = token, "client disconnected");
    }
}

impl<H: Handler> Drop for Server<H> {
    /// Removes the socket file so no stale socket is left behind.
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// The epoll token for a stream: its fd number, which is unique while open.
#[allow(clippy::cast_sign_loss)] // fds from the kernel are always >= 0
fn fd_token(stream: &impl AsRawFd) -> u64 {
    stream.as_raw_fd() as u64
}
