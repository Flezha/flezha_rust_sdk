use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::str;

use crate::processor::PayloadMetadata;

pub fn extract_payload<R: Read>(
    mut reader: R,
) -> io::Result<(String, Vec<u8>, HashMap<String, String>)> {
    // Read the payload ID (32 bytes)
    let mut id_buf = [0; 32];
    reader.read_exact(&mut id_buf)?;
    let payload_id = String::from_utf8_lossy(&id_buf).trim_end().to_string();

    // Read the payload size (4 bytes)
    let payload_length = reader.read_u32::<BigEndian>()? as usize;

    // Read the entire payload
    let mut payload_buf = vec![0; payload_length];
    reader.read_exact(&mut payload_buf)?;

    // Parse the payload: message_size (4) + message + metadata_size (2) + metadata
    let mut payload_reader = &payload_buf[..];

    // Read the message size (4 bytes)
    let message_length = payload_reader.read_u32::<BigEndian>()? as usize;

    // Read the actual message
    let mut message_buf = vec![0; message_length];
    payload_reader.read_exact(&mut message_buf)?;

    // Read the metadata size (2 bytes)
    let metadata_length = payload_reader.read_u16::<BigEndian>()? as usize;

    // Read the metadata
    let mut metadata_buf = vec![0; metadata_length];
    payload_reader.read_exact(&mut metadata_buf)?;

    // Deserialize metadata
    let metadata = deserialize_metadata(&metadata_buf)?;

    Ok((payload_id, message_buf, metadata))
}

pub fn extract_payload_non_blocking<R: Read>(
    mut reader: R,
) -> io::Result<Option<(String, Vec<u8>, HashMap<String, String>)>> {
    // New header format: payload_id (32) + payload_size (4) = 36 bytes
    let mut header_buf = [0; 36];
    match reader.read(&mut header_buf) {
        Ok(0) => {
            // End of stream
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Connection closed",
            ));
        }
        Ok(n) if n < 36 => {
            // This should not happen with UnixStream, but as a precaution.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Incomplete header",
            ));
        }
        Ok(_) => {
            // Header read successfully
        }
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
            return Ok(None);
        }
        Err(e) => {
            return Err(e);
        }
    }

    let id_buf = &header_buf[0..32];
    let payload_id = String::from_utf8_lossy(id_buf).trim_end().to_string();
    let payload_length = (&header_buf[32..]).read_u32::<BigEndian>()? as usize;

    // Read the entire payload
    let mut payload_buf = vec![0; payload_length];
    if payload_length > 0 {
        match reader.read_exact(&mut payload_buf) {
            Ok(_) => {}
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // This is a protocol error. We got a header but can't get the body.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Partial payload read",
                ));
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    // Parse the payload: message_size (4) + message + metadata_size (2) + metadata
    let mut payload_reader = &payload_buf[..];

    // Read the message size (4 bytes)
    let message_length = payload_reader.read_u32::<BigEndian>()? as usize;

    // Read the actual message
    let mut message_buf = vec![0; message_length];
    payload_reader.read_exact(&mut message_buf)?;

    // Read the metadata size (2 bytes)
    let metadata_length = payload_reader.read_u16::<BigEndian>()? as usize;

    // Read the metadata
    let mut metadata_buf = vec![0; metadata_length];
    payload_reader.read_exact(&mut metadata_buf)?;

    // Deserialize metadata
    let metadata = deserialize_metadata(&metadata_buf)?;

    Ok(Some((payload_id, message_buf, metadata)))
}

pub fn build_from_binary(payload_id: &str, message: &[u8], metadata: PayloadMetadata) -> Vec<u8> {
    build_payload(payload_id, message, metadata)
}

pub fn build_from_str(payload_id: &str, message: &str, metadata: PayloadMetadata) -> Vec<u8> {
    build_payload(payload_id, message.as_bytes(), metadata)
}

fn build_payload(payload_id: &str, message: &[u8], metadata: PayloadMetadata) -> Vec<u8> {
    // Serialize metadata
    let metadata_binary = serialize_metadata(&metadata);
    let metadata_size = metadata_binary.len() as u16;
    let message_size = message.len() as u32;

    // Calculate payload size: message_size (4) + message + metadata_size (2) + metadata
    let payload_size = 4 + message.len() + 2 + metadata_binary.len();

    // Ensure the payload ID is 32 bytes
    let mut payload_id_padded = payload_id.to_string();
    if payload_id_padded.len() > 32 {
        payload_id_padded.truncate(32);
    } else {
        payload_id_padded.push_str(&" ".repeat(32 - payload_id_padded.len()));
    }

    // Calculate total capacity: payload_id (32) + payload_size (4) + payload_size
    let total_capacity = 32 + 4 + payload_size;
    let mut result = Vec::with_capacity(total_capacity);

    // Write the payload ID (32 bytes)
    result.write_all(payload_id_padded.as_bytes()).unwrap();

    // Write the payload size (4 bytes)
    result.write_u32::<BigEndian>(payload_size as u32).unwrap();

    // Write the message size (4 bytes)
    result.write_u32::<BigEndian>(message_size).unwrap();

    // Write the message
    result.write_all(message).unwrap();

    // Write the metadata size (2 bytes)
    result.write_u16::<BigEndian>(metadata_size).unwrap();

    // Write the metadata
    result.write_all(&metadata_binary).unwrap();

    result
}

fn serialize_metadata(metadata: &HashMap<String, String>) -> Vec<u8> {
    let mut result = Vec::new();

    for (key, value) in metadata {
        let key_bytes = key.as_bytes();
        let value_bytes = value.as_bytes();

        // Write key size (1 byte)
        result.push(key_bytes.len() as u8);
        // Write value size (1 byte)
        result.push(value_bytes.len() as u8);
        // Write key
        result.extend_from_slice(key_bytes);
        // Write value
        result.extend_from_slice(value_bytes);
    }

    result
}

fn deserialize_metadata(metadata_bytes: &[u8]) -> io::Result<HashMap<String, String>> {
    let mut metadata = HashMap::new();
    let mut offset = 0;

    while offset < metadata_bytes.len() {
        if offset + 2 > metadata_bytes.len() {
            break; // Not enough bytes for key and value sizes
        }

        let key_size = metadata_bytes[offset] as usize;
        let value_size = metadata_bytes[offset + 1] as usize;
        offset += 2;

        if offset + key_size + value_size > metadata_bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid metadata format: insufficient bytes for key/value",
            ));
        }

        let key = String::from_utf8_lossy(&metadata_bytes[offset..offset + key_size]).to_string();
        offset += key_size;

        let value =
            String::from_utf8_lossy(&metadata_bytes[offset..offset + value_size]).to_string();
        offset += value_size;

        metadata.insert(key, value);
    }

    Ok(metadata)
}
