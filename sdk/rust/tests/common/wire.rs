//! Observing what a dispatch actually put on the wire: a socketpair whose
//! read half the test keeps, and the payloader frame parser that turns those
//! bytes back into `(message, metadata)`.

use std::collections::HashMap;
use std::io::Read;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flz_transport::transport::FlezhaStream;

use super::wait_until;

/// Socketpair whose write half is the dispatch stream and whose read half
/// lets the test observe emitted plugin frames.
pub fn stream_pair() -> (Arc<FlezhaStream>, UnixStream) {
    let (a, b) = UnixStream::pair().expect("UnixStream::pair");
    b.set_nonblocking(true).expect("set_nonblocking");
    (Arc::new(FlezhaStream::Unix(a)), b)
}

pub struct WireFrame {
    pub raw: Vec<u8>,
    pub message: Vec<u8>,
    pub metadata: HashMap<String, String>,
}

/// Parse complete payloader frames from a byte buffer:
/// `[payload_id(32)][payload_size(4)][message_size(4)][message]
///  [metadata_size(2)][metadata]`, metadata pairs
/// `[key_len(1)][val_len(1)][key][value]`.
pub fn parse_frames(mut bytes: &[u8]) -> Vec<WireFrame> {
    let mut out = Vec::new();
    while bytes.len() >= 36 {
        let payload_size = u32::from_be_bytes(bytes[32..36].try_into().unwrap()) as usize;
        if bytes.len() < 36 + payload_size {
            break;
        }
        let raw = bytes[..36 + payload_size].to_vec();
        let body = &bytes[36..36 + payload_size];
        let message_size = u32::from_be_bytes(body[..4].try_into().unwrap()) as usize;
        let message = body[4..4 + message_size].to_vec();
        let meta_size =
            u16::from_be_bytes(body[4 + message_size..6 + message_size].try_into().unwrap())
                as usize;
        let mut meta_bytes = &body[6 + message_size..6 + message_size + meta_size];
        let mut metadata = HashMap::new();
        while meta_bytes.len() >= 2 {
            let klen = meta_bytes[0] as usize;
            let vlen = meta_bytes[1] as usize;
            let key = String::from_utf8_lossy(&meta_bytes[2..2 + klen]).to_string();
            let val = String::from_utf8_lossy(&meta_bytes[2 + klen..2 + klen + vlen]).to_string();
            metadata.insert(key, val);
            meta_bytes = &meta_bytes[2 + klen + vlen..];
        }
        out.push(WireFrame {
            raw,
            message,
            metadata,
        });
        bytes = &bytes[36 + payload_size..];
    }
    out
}

/// Read from the observer socket until `expected` frames parse (or panic
/// after `timeout`).
pub fn read_frames(sock: &mut UnixStream, expected: usize, timeout: Duration) -> Vec<WireFrame> {
    let deadline = Instant::now() + timeout;
    let buf = Vec::new();
    let buf = std::cell::RefCell::new(buf);
    let sock = std::cell::RefCell::new(sock);
    let arrived = wait_until(
        || {
            let mut tmp = [0u8; 4096];
            match sock.borrow_mut().read(&mut tmp) {
                Ok(0) => {}
                Ok(n) => buf.borrow_mut().extend_from_slice(&tmp[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(e) => panic!("frame read failed: {e}"),
            }
            parse_frames(&buf.borrow()).len() >= expected
        },
        deadline.saturating_duration_since(Instant::now()),
    );
    let frames = parse_frames(&buf.borrow());
    assert!(
        arrived,
        "timed out with {} of {} frames",
        frames.len(),
        expected
    );
    frames
}
