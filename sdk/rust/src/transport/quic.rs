use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};

use quinn::{Connection, RecvStream, SendStream};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

/// Holds a QUIC connection and its backing tokio runtime.
pub struct QuicClient {
    runtime: Arc<Runtime>,
    connection: Connection,
}

impl QuicClient {
    /// Connect to a QUIC server at the given address using the CA cert for verification.
    pub fn connect(address: &str, ca_cert_path: &str) -> io::Result<QuicClient> {
        let runtime = Runtime::new()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let connection = runtime.block_on(async {
            let ca_cert_pem = std::fs::read(ca_cert_path)?;
            let mut reader = std::io::BufReader::new(&ca_cert_pem[..]);

            let mut root_store = rustls::RootCertStore::empty();
            for cert in rustls_pemfile::certs(&mut reader) {
                let cert = cert.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                root_store.add(cert)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            }

            let mut crypto = rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            crypto.alpn_protocols = vec![b"flezha".to_vec()];

            let mut client_config = quinn::ClientConfig::new(Arc::new(
                quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
            ));

            let mut transport_config = quinn::TransportConfig::default();
            transport_config.keep_alive_interval(Some(std::time::Duration::from_secs(5)));
            transport_config.stream_receive_window(8_388_608u32.into());
            transport_config.receive_window(16_777_216u32.into());
            transport_config.send_window(8_388_608);
            client_config.transport_config(Arc::new(transport_config));

            let mut endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            endpoint.set_default_client_config(client_config);

            let addr = address.parse()
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid address: {}", e)))?;

            let connection = endpoint
                .connect(addr, "localhost")
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;

            Ok::<Connection, io::Error>(connection)
        })?;

        Ok(QuicClient {
            runtime: Arc::new(runtime),
            connection,
        })
    }

    /// Open a new bidirectional stream on this connection.
    /// Spawns dedicated async reader/writer tasks that drive all QUIC I/O on the
    /// tokio runtime, completely decoupled from caller threads.
    pub fn open_bidi_stream(&self) -> io::Result<QuicBidiStream> {
        let (send, recv) = self.runtime.block_on(async {
            self.connection
                .open_bi()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        })?;

        Ok(QuicBidiStream::new(send, recv, Arc::clone(&self.runtime)))
    }

    /// Accept an incoming bidirectional stream from the peer.
    pub fn accept_bidi_stream(&self) -> io::Result<QuicBidiStream> {
        let (send, recv) = self.runtime.block_on(async {
            self.connection
                .accept_bi()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        })?;

        Ok(QuicBidiStream::new(send, recv, Arc::clone(&self.runtime)))
    }
}

/// Buffered reader backed by a tokio mpsc channel.
/// An async reader task fills the channel; this struct provides a sync Read
/// interface by calling `blocking_recv` — no tokio runtime involvement.
struct ChannelReader {
    rx: mpsc::Receiver<Vec<u8>>,
    buf: Vec<u8>,
    pos: usize,
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        // Serve from buffered chunk if available
        if self.pos < self.buf.len() {
            let n = std::cmp::min(out.len(), self.buf.len() - self.pos);
            out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
            self.pos += n;
            return Ok(n);
        }

        // Buffer exhausted — wait for next chunk from the async reader task
        match self.rx.blocking_recv() {
            Some(chunk) => {
                let n = std::cmp::min(out.len(), chunk.len());
                out[..n].copy_from_slice(&chunk[..n]);
                if n < chunk.len() {
                    self.buf = chunk;
                    self.pos = n;
                } else {
                    self.buf = Vec::new();
                    self.pos = 0;
                }
                Ok(n)
            }
            None => Ok(0), // Channel closed = stream finished
        }
    }
}

/// A bidirectional QUIC stream that implements std::io::Read and Write.
///
/// All QUIC I/O is driven by async tasks on the tokio runtime:
/// - An async **writer task** drains an mpsc channel into the QUIC SendStream.
/// - An async **reader task** reads from the QUIC RecvStream into an mpsc channel.
///
/// Caller threads never call `block_on` — writes push into a channel (instant),
/// reads block on a channel recv (pure sync, no runtime contention).
pub struct QuicBidiStream {
    write_tx: mpsc::UnboundedSender<Vec<u8>>,
    recv: Arc<Mutex<ChannelReader>>,
    runtime: Arc<Runtime>,
}

impl QuicBidiStream {
    fn new(send: SendStream, recv: RecvStream, runtime: Arc<Runtime>) -> Self {
        // Async writer task — owns the SendStream
        let (write_tx, write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        runtime.spawn(Self::writer_task(send, write_rx));

        // Async reader task — owns the RecvStream
        // Bounded channel (64 chunks × 64KB = ~4MB buffer) provides backpressure.
        let (read_tx, read_rx) = mpsc::channel::<Vec<u8>>(64);
        runtime.spawn(Self::reader_task(recv, read_tx));

        QuicBidiStream {
            write_tx,
            recv: Arc::new(Mutex::new(ChannelReader {
                rx: read_rx,
                buf: Vec::new(),
                pos: 0,
            })),
            runtime,
        }
    }

    async fn writer_task(mut send: SendStream, mut rx: mpsc::UnboundedReceiver<Vec<u8>>) {
        while let Some(data) = rx.recv().await {
            if send.write_all(&data).await.is_err() {
                break;
            }
        }
        let _ = send.finish();
    }

    async fn reader_task(mut recv: RecvStream, tx: mpsc::Sender<Vec<u8>>) {
        let mut buf = vec![0u8; 65536];
        loop {
            match recv.read(&mut buf).await {
                Ok(Some(n)) => {
                    if tx.send(buf[..n].to_vec()).await.is_err() {
                        break; // Consumer dropped
                    }
                }
                Ok(None) => break, // Stream finished
                Err(_) => break,   // Stream error
            }
        }
    }

    /// Clone by sharing the write channel and read channel.
    pub fn try_clone(&self) -> QuicBidiStream {
        QuicBidiStream {
            write_tx: self.write_tx.clone(),
            recv: Arc::clone(&self.recv),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

impl Read for QuicBidiStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.recv.lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?
            .read(buf)
    }
}

impl Write for QuicBidiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len();
        self.write_tx.send(buf.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "QUIC write channel closed"))?;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Read/Write for &QuicBidiStream (needed for Arc<FlezhaStream>)
impl Read for &QuicBidiStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.recv.lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?
            .read(buf)
    }
}

impl Write for &QuicBidiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len();
        self.write_tx.send(buf.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "QUIC write channel closed"))?;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
