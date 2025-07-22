use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};
use std::str;

pub fn extract_payload<R: Read>(mut reader: R) -> io::Result<(String, Vec<u8>)> {
    // Read the payload ID (32 bytes).
    let mut id_buf = [0; 32];
    reader.read_exact(&mut id_buf)?;
    let payload_id = String::from_utf8_lossy(&id_buf).trim_end().to_string();

    // Read the virtual channel (1 byte).
    let mut virtual_channel_buf = [0; 1];
    reader.read_exact(&mut virtual_channel_buf)?;

    // TODO: do something with the virtual channel.
    let _virtual_channel = virtual_channel_buf[0];

    // Read the length prefix (8 bytes).
    let mut length_buf = [0; 8];
    reader.read_exact(&mut length_buf)?;
    let message_length = (&length_buf[..]).read_u64::<BigEndian>()? as usize;

    // Read the actual message.
    let mut message_buf = vec![0; message_length];
    reader.read_exact(&mut message_buf)?;

    Ok((payload_id, message_buf))
}

pub fn extract_payload_non_blocking<R: Read>(
    mut reader: R,
) -> io::Result<Option<(String, Vec<u8>)>> {
    let mut header_buf = [0; 41];
    match reader.read(&mut header_buf) {
        Ok(0) => {
            // End of stream
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "Connection closed"));
        }
        Ok(n) if n < 41 => {
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
    let _virtual_channel = header_buf[32];
    let message_length = (&header_buf[33..]).read_u64::<BigEndian>()? as usize;

    let mut message_buf = vec![0; message_length];
    if message_length > 0 {
        match reader.read_exact(&mut message_buf) {
            Ok(_) => {}
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // This is a protocol error. We got a header but can't get the body.
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Partial message read",
                ));
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    Ok(Some((payload_id, message_buf)))
}

pub fn build_from_binary(payload_id: &str, virtual_channel: u8, message: &[u8]) -> Vec<u8> {
    build_payload(payload_id, virtual_channel, message)
}

pub fn build_from_str(payload_id: &str, virtual_channel: u8, message: &str) -> Vec<u8> {
    build_payload(payload_id, virtual_channel, message.as_bytes())
}

fn build_payload(payload_id: &str, virtual_channel: u8, message: &[u8]) -> Vec<u8> {
    let message_len = message.len() as u64;
    let mut payload = handle_payload_build(payload_id, virtual_channel, message_len);
    payload.write_all(message).unwrap();

    payload
}

fn handle_payload_build(payload_id: &str, virtual_channel: u8, message_len: u64) -> Vec<u8> {
    // Ensure the payload ID is 32 bytes.
    let mut payload_id_padded = payload_id.to_string();
    if payload_id_padded.len() > 32 {
        payload_id_padded.truncate(32);
    } else {
        payload_id_padded.push_str(&" ".repeat(32 - payload_id_padded.len()));
    }

    // Create a buffer to hold the payload.
    let mut payload = Vec::with_capacity(32 + 8 + message_len as usize);

    // Write the payload ID (32 bytes) to the payload.
    payload.write_all(payload_id_padded.as_bytes()).unwrap();

    // Write the virtual channel (1 byte) to the payload.
    payload.push(virtual_channel);

    // Write the message size (8 bytes) to the payload.
    let _ = payload.write_u64::<BigEndian>(message_len);

    payload
}
