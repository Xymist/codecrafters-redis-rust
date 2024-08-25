#![allow(dead_code)]

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

#[derive(Debug, PartialEq)]
enum RESPValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RESPValue>),
}

fn parse_input(input: &str) -> Vec<RESPValue> {
    let mut parts = input.split(SEPARATOR).peekable();
    let mut values = Vec::new();

    while parts.peek().is_some() && !parts.peek().unwrap().is_empty() {
        println!("part: {:?}", parts.peek());
        values.push(parse_input_segments(&mut parts));
    }

    values
}

fn parse_input_segments<'a>(parts: &mut impl Iterator<Item = &'a str>) -> RESPValue {
    let mut chars = parts.next().unwrap().chars();
    println!("chars: {:?}", chars);
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
