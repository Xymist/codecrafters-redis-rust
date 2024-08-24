use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener},
};

fn main() {
    bind_and_listen();
}

fn bind_and_listen() {
    let listener = TcpListener::bind("127.0.0.1:6380").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = String::new();
                stream
                    .set_read_timeout(Some(std::time::Duration::from_millis(100)))
                    .unwrap();

                loop {
                    match stream.read_to_string(&mut buf) {
                        Ok(byte_count) if byte_count > 0 => byte_count,
                        Ok(_) => {
                            println!("Received 0 bytes");
                            break;
                        }
                        Err(_) => {
                            break;
                        }
                    };
                }

                println!("Stream: {:?}", buf);

                stream.write_all(b"+PONG\r\n").unwrap();
                stream.flush().unwrap();
                stream.shutdown(Shutdown::Both).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
