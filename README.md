# socket-epoll-server

A small, single-threaded Unix domain socket server built directly on Linux
`epoll`. You bring the protocol by implementing the `Handler` trait. The
library handles accepting clients, non-blocking I/O, buffering and cleanup.

> **Platform:** Linux only, because it uses `epoll`.

## Features

- One thread and one `epoll` instance serve many clients, with no async runtime
- Per-connection application state (`Handler::State`)
- Input buffering: return how many bytes you consumed, and the rest is kept
  and passed back to you when more data arrives, so framing is easy
- Buffered output through `Connection::send`
- The socket file is removed before binding and again when the `Server` is dropped
- Logging through [`tracing`](https://crates.io/crates/tracing)

## Installation

```toml
[dependencies]
socket-epoll-server = "0.1"
```

## Example: line-based echo server

```rust,no_run
use socket_epoll_server::{Connection, Handler, Server};

struct Echo;

#[derive(Default)]
struct State {
    lines: usize,
}

impl Handler for Echo {
    type State = State;

    fn on_connect(&mut self, conn: &mut Connection<State>) {
        conn.send(b"welcome\n");
    }

    fn on_data(&mut self, conn: &mut Connection<State>, input: &[u8]) -> usize {
        let mut consumed = 0;
        // Handle every complete line; keep a trailing partial line for later.
        while let Some(pos) = input[consumed..].iter().position(|&b| b == b'\n') {
            let line = &input[consumed..consumed + pos];
            if line == b"quit" {
                conn.close();
                return input.len();
            }
            conn.state.lines += 1;
            conn.send(line);
            conn.send(b"\n");
            consumed += pos + 1;
        }
        consumed
    }

    fn on_close(&mut self, conn: &mut Connection<State>) {
        println!("client {} left after {} lines", conn.id(), conn.state.lines);
    }
}

fn main() -> std::io::Result<()> {
    let mut server = Server::bind("/tmp/echo.sock", Echo)?;
    server.run()
}
```

Try it:

```sh
socat - UNIX-CONNECT:/tmp/echo.sock
# or: nc -U /tmp/echo.sock
```

## API overview

| Item | Purpose |
|------|---------|
| `Server::bind(path, handler)` | Creates the listening socket, replacing any stale file at `path` |
| `Server::run()` | Runs the event loop. It returns only if `epoll_wait` fails |
| `Handler::on_connect` | Optional. Called once after a client connects |
| `Handler::on_data` | Required. Receives all unconsumed input and returns the number of bytes consumed |
| `Handler::on_close` | Optional. Called once before the connection is dropped. You can still `send` a final reply |
| `Connection::send` | Queues bytes to send to the client |
| `Connection::close` | Closes the connection after the current event |
| `Connection::id` | Identifier of the connection while it is open |
| `Connection::state` | Your per-connection data (`H::State`) |

Errors on individual clients (a failed read, write or accept) are logged with
`tracing` and only drop that client. The server keeps running.

## Logging

The crate emits `tracing` events but does not install a subscriber. To see
the logs, install one in your binary:

```rust,ignore
tracing_subscriber::fmt()
    .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    .init();
```

```sh
RUST_LOG=socket_epoll_server=trace cargo run
```

## Limitations

- Linux only.
- Single-threaded: a slow `Handler` blocks every client.
- `EPOLLOUT` is not handled yet. Output the socket cannot accept right away
  stays buffered and is only retried on the client's next read event.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
