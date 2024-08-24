use core::str;
use std::{
    io::{self, BufRead, Write},
    net::{Shutdown, TcpListener},
    time::Duration,
};

fn main() {
    bind_and_listen();
}

fn bind_and_listen() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

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
                println!("received: {}", s);
                buf.clear();
                println!("agg: {}", agg);

                if agg.ends_with("PING\r\n") {
                    stream.write_all(b"+PONG\r\n").unwrap();
                    agg.clear();
                    stream.flush().unwrap();
                    break;
                }

                if agg.ends_with("COMMAND\r\n") {
                    stream.write_all(b"+OK\r\n").unwrap();
                    agg.clear();
                    stream.flush().unwrap();
                    break;
                }

                if agg.ends_with("QUIT\r\n") {
                    stream.write_all(b"+OK\r\n").unwrap();
                    agg.clear();
                    stream.flush().unwrap();
                    break;
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                println!("would block");
                continue;
            }
            Err(e) => {
                println!("error: {}", e);
                continue;
            }
        }
    }

    stream.flush().unwrap();
    stream.shutdown(Shutdown::Both).unwrap();
}
