mod protocol_parser;

use core::str;
use std::{
    io::{self, Read, Write},
    net::{Shutdown, TcpListener},
};

use protocol_parser::parse_input;

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
    const BUFFER_SIZE: usize = 10;
    let mut agg = String::new();
    let mut buf = [0; BUFFER_SIZE];
    let mut reader = io::BufReader::new(stream.try_clone().unwrap());

    loop {
        match reader.read(&mut buf) {
            // This is the last segment, either a partial buffer or
            // completely empty if the last full buffer was a perfect fit.
            Ok(n) if n < BUFFER_SIZE => {
                // If the buffer is empty and we didn't read anything last time,
                // we're just holding the connection open for more commands.
                if agg.is_empty() && n == 0 {
                    continue;
                }

                let s = str::from_utf8(&buf[..n]).unwrap();
                agg.push_str(s);
                println!("agg: {:?}", agg);
                let commands = parse_input(&agg);
                for command in commands {
                    let response = command.into_command().into_response();
                    stream.write_all(response.to_string().as_bytes()).unwrap();
                }
                agg.clear();
            }
            // This is a full buffer, we need to keep reading.
            Ok(n) => {
                let s = str::from_utf8(&buf[..n]).unwrap();
                agg.push_str(s);
                buf.fill(0);
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
