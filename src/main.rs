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
        println!("new connection");
        match stream {
            Ok(mut stream) => {
                std::thread::spawn(move || handle_connection(&mut stream));
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

                if is_ping(&agg) {
                    stream.write_all(b"+PONG\r\n").unwrap();
                    agg.clear();
                    stream.flush().unwrap();
                    continue;
                }

                if is_command(&agg) {
                    stream.write_all(b"+OK\r\n").unwrap();
                    agg.clear();
                    stream.flush().unwrap();
                    continue;
                }

                if is_quit(&agg) {
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

fn is_ping(s: &str) -> bool {
    s.ends_with("PING\r\n") || s.ends_with("ping\r\n")
}

fn is_command(s: &str) -> bool {
    s.ends_with("COMMAND\r\n") || s.ends_with("command\r\n")
}

fn is_quit(s: &str) -> bool {
    s.ends_with("QUIT\r\n") || s.ends_with("quit\r\n")
}
