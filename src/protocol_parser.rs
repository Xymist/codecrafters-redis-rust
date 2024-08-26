#![allow(dead_code)]

use std::fmt::Display;

const SEPARATOR: &str = "\r\n";
const SIMPLE_STRING_PREFIX: char = '+';
const SIMPLE_ERROR_PREFIX: char = '-';
const INTEGER_PREFIX: char = ':';
const BULK_STRING_PREFIX: char = '$';
const ARRAY_PREFIX: char = '*';
const NULL_PREFIX: char = '_';
const BOOLEAN_PREFIX: char = '#';
const DOUBLE_PREFIX: char = ',';
const BIG_NUMBER_PREFIX: char = '(';
const BULK_ERROR_PREFIX: char = '!';
const VERBATIM_STRING_PREFIX: char = '=';
const MAP_PREFIX: char = '%';
const SET_PREFIX: char = '~';
const PUSH_PREFIX: char = '>';

pub enum Command {
    Ping,
    Echo(RESPValue),
    #[allow(clippy::enum_variant_names)]
    Command,
    Set {
        key: String,
        value: RESPValue,
    },
    Get(String),
}

impl Command {
    pub fn into_response(self) -> Response {
        match self {
            Command::Ping => Response::Pong,
            Command::Echo(s) => Response::Echo(s),
            Command::Command => Response::Ok,
            Command::Set { key: _, value: _ } => Response::Ok,
            Command::Get(key) => {
                let res = super::db_get(key.clone());
                match res {
                    Some(value) => dbg!(Response::Echo(value)),
                    None => Response::Null,
                }
            }
        }
    }

    pub fn execute(&self) {
        match self {
            Command::Ping => println!("PONG"),
            Command::Echo(s) => println!("{}", s),
            Command::Command => println!("COMMAND"),
            Command::Set { key, value } => {
                println!("SET {} {:?}", key, value);
                super::db_set(key.clone(), value.clone());
            }
            Command::Get(key) => {
                println!("GET {}", key);
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Response {
    Ok,
    Pong,
    Echo(RESPValue),
    Null,
}

impl Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Response::Ok => write!(f, "+OK\r\n"),
            Response::Pong => write!(f, "+PONG\r\n"),
            Response::Echo(s) => write!(f, "{}", s),
            Response::Null => write!(f, "$-1\r\n"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum RESPValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RESPValue>),
}

impl Display for RESPValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RESPValue::SimpleString(s) => write!(f, "+{}\r\n", s),
            RESPValue::Error(s) => write!(f, "-{}\r\n", s),
            RESPValue::Integer(i) => write!(f, ":{}\r\n", i),
            RESPValue::BulkString(s) => write!(f, "${}\r\n{}\r\n", s.len(), s),
            RESPValue::Array(values) => {
                write!(f, "*{}\r\n", values.len())?;
                for value in values {
                    write!(f, "{}", value)?;
                }
                Ok(())
            }
        }
    }
}

impl RESPValue {
    pub fn into_command(self) -> Command {
        match self {
            RESPValue::SimpleString(command) => match command.as_str() {
                "PING" => Command::Ping,
                "COMMAND" => Command::Command,
                _ => unimplemented!(),
            },
            RESPValue::Array(values) => {
                let mut iter = values.into_iter();
                let first = iter.next().unwrap();

                match first {
                    RESPValue::BulkString(command) => match command.to_ascii_uppercase().as_str() {
                        "ECHO" => Command::Echo(iter.next().unwrap()),
                        "PING" => Command::Ping,
                        "COMMAND" => Command::Command,
                        "SET" => {
                            let key = match iter.next().unwrap() {
                                RESPValue::BulkString(s) => s,
                                _ => unimplemented!(),
                            };
                            let value = iter.next().unwrap();

                            Command::Set { key, value }
                        }
                        "GET" => {
                            let key = match iter.next().unwrap() {
                                RESPValue::BulkString(s) => s,
                                _ => unimplemented!(),
                            };

                            Command::Get(key)
                        }
                        _ => unimplemented!(),
                    },
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }
}

pub fn parse_input(input: &str) -> Vec<RESPValue> {
    let mut parts = input.split(SEPARATOR).peekable();
    let mut values = Vec::new();

    while parts.peek().is_some() && !parts.peek().unwrap().is_empty() {
        values.push(parse_input_segments(&mut parts));
    }

    values
}

fn parse_input_segments<'a>(parts: &mut impl Iterator<Item = &'a str>) -> RESPValue {
    let mut chars = parts.next().unwrap().chars();
    let prefix = chars.next().unwrap();
    let rest = chars.as_str();

    match prefix {
        SIMPLE_STRING_PREFIX => RESPValue::SimpleString(rest.to_string()),
        SIMPLE_ERROR_PREFIX => RESPValue::Error(rest.to_string()),
        INTEGER_PREFIX => RESPValue::Integer(rest.parse().unwrap()),
        // We could use the number to double check here, but we already split by the line break,
        // so we know the entire next value is the string we want.
        BULK_STRING_PREFIX => RESPValue::BulkString(parts.next().unwrap().to_string()),
        ARRAY_PREFIX => {
            let len: usize = rest.parse().unwrap();
            let mut values = Vec::new();

            for _ in 0..len {
                let value = parse_input_segments(parts);
                values.push(value);
            }
            RESPValue::Array(values)
        }
        _ => panic!("Unknown prefix: {}", prefix),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping() {
        let input = "+PING\r\n";
        assert_eq!(
            parse_input(input),
            vec![RESPValue::SimpleString(String::from("PING"))]
        );
    }

    #[test]
    fn test_echo() {
        let input = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
        assert_eq!(
            parse_input(input),
            vec![RESPValue::Array(vec![
                RESPValue::BulkString(String::from("ECHO")),
                RESPValue::BulkString(String::from("hey"))
            ])]
        );
    }

    #[test]
    fn test_multiple_commands() {
        let input = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n*2\r\n$4\r\nECHO\r\n$3\r\nyou\r\n";
        assert_eq!(
            parse_input(input),
            vec![
                RESPValue::Array(vec![
                    RESPValue::BulkString(String::from("ECHO")),
                    RESPValue::BulkString(String::from("hey"))
                ]),
                RESPValue::Array(vec![
                    RESPValue::BulkString(String::from("ECHO")),
                    RESPValue::BulkString(String::from("you"))
                ])
            ]
        );
    }
}
