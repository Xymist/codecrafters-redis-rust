use core::str;
use std::{
    io::{self, BufRead, Write},
    net::{Shutdown, TcpListener},
};

fn main() {
    let mut args = std::env::args();
    let port = args.nth(1).unwrap_or("6379".to_string());
    bind_and_listen(port);
}

fn bind_and_listen(port: String) {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                handle_connection(&mut stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_connection(stream: &mut std::net::TcpStream) {
    let mut agg = String::new();
    let mut buf = Vec::new();
    let mut reader = io::BufReader::new(stream.try_clone().unwrap());

    loop {
        match reader.read_until(b'\n', &mut buf) {
            Ok(0) => {
                break;
            }
            Ok(n) => {
                let s = str::from_utf8(&buf[..n]).unwrap();
                agg.push_str(s);
                buf.clear();

                if agg.ends_with("PING\r\n") {
                    stream.write_all(b"+PONG\r\n").unwrap();
                    agg.clear();
                    stream.flush().unwrap();
                    continue;
                }

                if agg.ends_with("COMMAND\r\n") {
                    stream.write_all(b"+OK\r\n").unwrap();
                    agg.clear();
                    stream.flush().unwrap();
                    continue;
                }

                if agg.ends_with("QUIT\r\n") {
                    stream.write_all(b"+OK\r\n").unwrap();
                    agg.clear();
                    stream.flush().unwrap();
                    continue;
                }
            }
            Err(e) => {
                println!("error: {}", e);
                break;
            }
        }
    }

    stream.flush().unwrap();
    stream.shutdown(Shutdown::Both).unwrap();
}
