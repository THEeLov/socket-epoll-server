use socket_epoll_server::{Connection, Handler, Server};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    thread,
    time::Duration,
};

struct LineEcho;
impl Handler for LineEcho {
    type State = ();
    fn on_data(&mut self, conn: &mut Connection<()>, input: &[u8]) -> usize {
        match input.iter().rposition(|&b| b == b'\n') {
            Some(pos) => {
                conn.send(&input[..=pos]);
                pos + 1
            }
            None => 0,
        }
    }
}

fn start(name: &str) -> UnixStream {
    // Unique path per test: tests run in parallel threads.
    let path = std::env::temp_dir().join(format!("{name}-{}.sock", std::process::id()));
    let server = Server::bind(&path, LineEcho).unwrap(); // bind before connecting
    thread::spawn(move || {
        let mut s = server;
        s.run()
    }); // run() never returns
    let client = UnixStream::connect(&path).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap(); // fail instead of hang
    client
}

#[test]
fn echoes_line_sent_in_two_parts() {
    let mut client = start("split");
    client.write_all(b"hel").unwrap();
    thread::sleep(Duration::from_millis(50));
    client.write_all(b"lo\n").unwrap();

    let mut line = String::new();
    BufReader::new(&client).read_line(&mut line).unwrap();
    assert_eq!(line, "hello\n");
}
