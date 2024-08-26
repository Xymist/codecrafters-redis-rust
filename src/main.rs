mod protocol_parser;

use core::str;
use protocol_parser::{parse_input, RESPValue, SetCondition, SetOpts};
use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::{Shutdown, TcpListener},
    sync::{Mutex, OnceLock},
    time::SystemTime,
};

#[derive(Debug, Clone, PartialEq)]
struct DBEntry {
    value: RESPValue,
    expires_at: Option<SystemTime>,
}

impl DBEntry {
    fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expires_at {
            let now = SystemTime::now();
            now > expiry
        } else {
            false
        }
    }
}

// TODO: There are expired keys that will never be accessed again. These keys should be expired anyway, so periodically
// Redis tests a few keys at random among keys with an expire set. All the keys that are already expired are deleted
// from the keyspace.
static DB: OnceLock<Mutex<HashMap<String, DBEntry>>> = OnceLock::new();

fn main() {
    let mut args = std::env::args();
    let port = args.nth(1).unwrap_or("6379".to_string());

    DB.get_or_init(|| Mutex::new(HashMap::new()));

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
                let inputs = parse_input(&agg);
                for input in inputs {
                    let command = input.into_command();
                    let response = command.as_response();
                    command.execute();
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

fn db_set(key: String, value: RESPValue, opts: &SetOpts) {
    let mut guard = DB.get().unwrap().lock().unwrap();
    let key_exists = guard.contains_key(&key);
    let condition = opts.condition();

    if key_exists && *condition == SetCondition::IfNotExists {
        return;
    }

    if !key_exists && *condition == SetCondition::IfExists {
        return;
    }

    let new_entry = DBEntry {
        value,
        expires_at: opts.expires_at(),
    };
    guard.insert(key, new_entry);
    println!("DB contents: {:?}", guard);
}

fn db_get(key: String) -> Option<RESPValue> {
    let mut guard = DB.get().unwrap().lock().unwrap();
    let entry = guard.get(&key).cloned();
    if let Some(entry) = entry {
        if entry.is_expired() {
            guard.remove(&key);
            return None;
        }
        Some(entry.value)
    } else {
        None
    }
}
