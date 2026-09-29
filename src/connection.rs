//! One connected client: its socket, its buffers and the application's
//! per-connection state.

use std::{
    io::{self, Read, Write},
    os::unix::net::UnixStream,
};

/// A connected client, as seen by a [`Handler`](crate::Handler).
///
/// The socket and buffers are private: the handler can only queue data with
/// [`send`](Connection::send), ask for the connection to be closed with
/// [`close`](Connection::close), and use its own [`state`](Connection::state).
pub struct Connection<S> {
    id: u64,
    stream: UnixStream,
    input: Vec<u8>,
    output: Vec<u8>,
    closed: bool,
    /// Per-connection data owned by the application.
    pub state: S,
}

impl<S> Connection<S> {
    pub(crate) fn new(id: u64, stream: UnixStream, state: S) -> Self {
        Self {
            id,
            stream,
            input: Vec::new(),
            output: Vec::new(),
            closed: false,
            state,
        }
    }

    /// A number identifying this connection while it is open.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Queues `data` to be sent to the client.
    pub fn send(&mut self, data: &[u8]) {
        self.output.extend_from_slice(data);
    }

    /// Asks the server to close this connection after the current event.
    pub fn close(&mut self) {
        self.closed = true;
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    pub(crate) fn stream(&self) -> &UnixStream {
        &self.stream
    }

    /// Reads whatever is available and appends it to the input buffer.
    /// Returns the number of bytes read; `Ok(0)` means the client hung up.
    pub(crate) fn read_available(&mut self) -> io::Result<usize> {
        let mut buffer = [0u8; 4096];
        let n = self.stream.read(&mut buffer)?;
        self.input.extend_from_slice(&buffer[..n]);
        Ok(n)
    }

    /// Hands the unprocessed input to `f` and removes the bytes it reports
    /// as consumed.
    pub(crate) fn process_input(&mut self, f: impl FnOnce(&mut Self, &[u8]) -> usize) {
        // Move the buffer out so `f` can get `&mut self` and `&input` at once.
        let input = std::mem::take(&mut self.input);
        let consumed = f(self, &input).min(input.len());
        self.input = input;
        self.input.drain(..consumed);
    }

    /// Writes as much of the output buffer as the socket accepts.
    ///
    /// Anything the socket does not accept yet stays buffered and is retried
    /// on the next event. (Proper `EPOLLOUT` handling is still to do.)
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        while !self.output.is_empty() {
            match self.stream.write(&self.output) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => {
                    self.output.drain(..n);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> (Connection<()>, UnixStream) {
        let (server, client) = UnixStream::pair().unwrap();
        server.set_nonblocking(true).unwrap();
        (Connection::new(1, server, ()), client)
    }

    #[test]
    fn unconsumed_input_is_kept_for_next_call() {
        let (mut conn, mut client) = pair();
        client.write_all(b"hello").unwrap();
        conn.read_available().unwrap();

        conn.process_input(|_, input| {
            assert_eq!(input, b"hello");
            2
        });
        conn.process_input(|_, input| {
            assert_eq!(input, b"llo");
            0
        });
    }

    #[test]
    fn read_returns_zero_when_client_hangs_up() {
        let (mut conn, client) = pair();
        drop(client);
        assert_eq!(conn.read_available().unwrap(), 0);
    }

    #[test]
    fn flush_sends_queued_output() {
        let (mut conn, mut client) = pair();
        conn.send(b"hi");
        conn.flush().unwrap();
        let mut buf = [0u8; 2];
        client.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"hi");
    }
}
