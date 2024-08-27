use crate::protocol_parser::RESPValue;
use anyhow::{bail, Result};
use core::str;
use std::{collections::HashMap, fs::File, io::Read, time::SystemTime, vec};

const MAGIC_STRING: &str = "REDIS";

#[derive(Debug, Clone, PartialEq)]
pub struct DBEntry {
    value: RESPValue,
    expires_at: Option<SystemTime>,
}

impl DBEntry {
    pub fn new(value: RESPValue, expires_at: Option<SystemTime>) -> Self {
        DBEntry { value, expires_at }
    }
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expires_at {
            let now = SystemTime::now();
            now > expiry
        } else {
            false
        }
    }
    pub fn value(&self) -> &RESPValue {
        &self.value
    }
}

#[derive(Debug, Default)]
pub struct Rdb {
    version: String,
    metadata: HashMap<String, String>,
    db_hash_table_size: usize,
    expiry_hash_table_size: usize,
    selected_db: u32,
    data: HashMap<String, DBEntry>,
    original_checksum: u64,
}

impl Rdb {
    pub fn data_mut(&mut self) -> &mut HashMap<String, DBEntry> {
        &mut self.data
    }
}

pub fn load_db() -> Result<Rdb> {
    let mut db_data = Rdb::default();
    let config = crate::args();
    let path = format!("{}/{}", config.directory, config.dbfilename);

    if !std::path::Path::new(&path).exists() {
        println!(
            "No RDB file found at {}. Starting with empty database.",
            path
        );
        return Ok(db_data);
    }

    let mut file = File::open(path)?;

    // Fetch the header section. This should be the magic string "REDIS" followed by a four-digit version number.
    let mut buf = [0; 9];
    file.read_exact(&mut buf)?;
    let magic = str::from_utf8(&buf[0..5])?;
    if magic != MAGIC_STRING {
        bail!("Invalid magic string: {}", magic);
    } else {
        let version = str::from_utf8(&buf[5..])?;
        db_data.version = version.to_string();
    }

    // Fetch the metadata section
    // FA                             // Indicates the start of a metadata subsection.
    // 09 72 65 64 69 73 2D 76 65 72  // The name of the metadata attribute (string encoded): "redis-ver".
    // 06 36 2E 30 2E 31 36           // The value of the metadata attribute (string encoded): "6.0.16".
    //
    // There may be zero or more metadata subsections.
    // Each subsection starts with the byte 0xFA and is followed by a null-terminated string that represents the name of the metadata attribute.
    // The value of the attribute is also a null-terminated string.
    // The metadata section is terminated by a null byte.
    loop {
        let mut buf = [0; 1];
        file.read_exact(&mut buf)?;

        if buf[0] == 0xFA {
            // Do a metadata section
            // We're looking for two strings, each preceded by a length byte,
            // the latter followed by a b11111010 (250) byte.
            let mut buf = [0; 1];
            file.read_exact(&mut buf)?;
            let key = extract_value(buf[0], &mut file)?;

            // Read the next byte, which should be the length of the value
            file.read_exact(&mut buf)?;
            let value = extract_value(buf[0], &mut file)?;

            db_data.metadata.insert(key, value);
        } else if buf[0] == 0xFE {
            // Do a database selector section
            let selected_db = {
                let mut buf = [0; 1];
                file.read_exact(&mut buf)?;
                extract_value(buf[0], &mut file)?
            };
            db_data.selected_db = selected_db.parse().unwrap_or(0);
        } else if buf[0] == 0xFB {
            // Do a resize database section
            let mut buf = [0; 1];

            file.read_exact(&mut buf)?;
            let db_size = extract_value(buf[0], &mut file)?;
            db_data.db_hash_table_size = db_size.parse().unwrap_or(0);

            file.read_exact(&mut buf)?;
            let expires_size = extract_value(buf[0], &mut file)?;
            db_data.expiry_hash_table_size = expires_size.parse().unwrap_or(0);
        } else if buf[0] == 0xFF {
            // End of file. Read an eight-byte checksum and we're done.
            let mut buf = [0; 8];
            file.read_exact(&mut buf)?;
            let checksum = u64::from_le_bytes(buf);
            db_data.original_checksum = checksum;

            break;
        } else {
            let expiry = match buf[0] {
                0xFD => {
                    let mut buf = [0; 4];
                    file.read_exact(&mut buf)?;
                    let expiry = u32::from_le_bytes(buf);
                    if expiry == 0 {
                        None
                    } else {
                        Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(expiry as u64))
                    }
                }
                0xFC => {
                    let mut buf = [0; 8];
                    file.read_exact(&mut buf)?;
                    let expiry = u64::from_le_bytes(buf);
                    if expiry == 0 {
                        continue;
                    } else {
                        Some(SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(expiry))
                    }
                }
                _ => None,
            };

            let key = {
                let key_start_byte = if expiry.is_some() {
                    let mut nbuf = [0; 1];
                    file.read_exact(&mut nbuf)?;
                    nbuf[0]
                } else {
                    buf[0]
                };

                extract_value(key_start_byte, &mut file)?
            };

            let value = {
                let mut buf = [0; 1];
                file.read_exact(&mut buf)?;
                let value = extract_value(buf[0], &mut file)?;
                // TODO: not everything is a string, this needs correcting
                RESPValue::SimpleString(value)
            };

            db_data.data.insert(key, DBEntry::new(value, expiry));
        }
    }
    if cfg!(debug_assertions) {
        println!("Loaded RDB file: {:#?}", db_data);
    }

    Ok(db_data)
}

// Incredibly stupid fucking encoding for how long the next
// item is which requires special handling.
//
// 00 	The next 6 bits represent the length
// 01 	Read one additional byte. The combined 14 bits represent the length
// 10 	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
// 11 	The next object is encoded in a special format. The remaining 6 bits indicate the format:
//   0 indicates that an 8 bit integer follows
//   1 indicates that a 16 bit integer follows
//   2 indicates that a 32 bit integer follows
//   3 indicates that a compressed string follows:
//     The compressed length clen is read from the stream using Length Encoding
//     The uncompressed length is read from the stream using Length Encoding
//     The next clen bytes are read from the stream
//     Finally, these bytes are decompressed using LZF algorithm
fn extract_value(byte: u8, file: &mut File) -> Result<String> {
    let nullified = byte & 0b11000000;

    match nullified {
        0b00000000 => {
            let remaining_bits = byte & 0b00111111;
            let length = remaining_bits as usize;
            if length == 0 {
                return Ok(String::from("0"));
            }
            let mut val = vec![0; length];
            file.read_exact(&mut val)?;
            // In this specific case, we need to convert 0 bytes to '0' bytes
            // because otherwise they get interpreted as string terminators
            // rather than integer values.
            val = val.iter().map(|&x| if x == 0 { b'0' } else { x }).collect();
            Ok(String::from_utf8(val)?)
        }
        0b01000000 => {
            let remaining_bits = byte & 0b00111111;
            let mut buf = [0; 1];
            file.read_exact(&mut buf)?;

            let length = u16::from_le_bytes([remaining_bits, buf[0]]) as usize;
            let mut val = vec![0; length];
            file.read_exact(&mut val)?;
            Ok(String::from_utf8(val)?)
        }
        0b10000000 => {
            let mut buf = [0; 4];
            file.read_exact(&mut buf)?;
            let length = u32::from_le_bytes(buf) as usize;
            let mut val = vec![0; length];
            file.read_exact(&mut val)?;
            Ok(String::from_utf8(val)?)
        }
        0b11000000..=0b11000010 => {
            let upcoming_bytes = match byte & 0b00111111 {
                0 => 1,
                1 => 2,
                2 => 4,
                _ => unreachable!(),
            };
            let mut buf = vec![0; upcoming_bytes];
            file.read_exact(&mut buf)?;
            let slice = &buf[0..upcoming_bytes];
            let encoded = match upcoming_bytes {
                1 => u8::from_le_bytes(slice.try_into().unwrap()) as usize,
                2 => u16::from_le_bytes(slice.try_into().unwrap()) as usize,
                4 => u32::from_le_bytes(slice.try_into().unwrap()) as usize,
                _ => unreachable!(),
            };
            Ok(encoded.to_string())
        }
        0b11000011 => {
            let mut buf = [0; 1];
            file.read_exact(&mut buf)?;
            let clen = extract_value(buf[0], file)?.parse::<usize>()?;
            //let ulen = extract_value(buf[0], file)?.parse::<usize>()?;
            let mut compressed = vec![0; clen];
            file.read_exact(&mut compressed)?;
            //let mut uncompressed = vec![0; ulen];
            //lzf::decompress(&compressed, &mut uncompressed)?;
            Ok(String::from_utf8(compressed)?)
        }
        _ => unreachable!(),
    }
}
