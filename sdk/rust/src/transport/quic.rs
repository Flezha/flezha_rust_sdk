use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};

use quinn::{Connection, RecvStream, SendStream};
use tokio::runtime::Runtime;

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
    pub fn open_bidi_stream(&self) -> io::Result<QuicBidiStream> {
        let (send, recv) = self.runtime.block_on(async {
            self.connection
                .open_bi()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        })?;

        Ok(QuicBidiStream {
            send: Arc::new(Mutex::new(send)),
            recv: Arc::new(Mutex::new(recv)),
            runtime: Arc::clone(&self.runtime),
        })
    }

    /// Accept an incoming bidirectional stream from the peer.
    pub fn accept_bidi_stream(&self) -> io::Result<QuicBidiStream> {
        let (send, recv) = self.runtime.block_on(async {
            self.connection
                .accept_bi()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
        })?;

        Ok(QuicBidiStream {
            send: Arc::new(Mutex::new(send)),
            recv: Arc::new(Mutex::new(recv)),
            runtime: Arc::clone(&self.runtime),
        })
    }
}

/// A bidirectional QUIC stream that implements std::io::Read and Write
/// by blocking on the tokio runtime.
pub struct QuicBidiStream {
    send: Arc<Mutex<SendStream>>,
    recv: Arc<Mutex<RecvStream>>,
    runtime: Arc<Runtime>,
}

impl QuicBidiStream {
    /// Clone by sharing the Arc references (mutex-protected).
    pub fn try_clone(&self) -> QuicBidiStream {
        QuicBidiStream {
            send: Arc::clone(&self.send),
            recv: Arc::clone(&self.recv),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

impl Read for QuicBidiStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut recv = self.recv.lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        self.runtime.block_on(async {
            match recv.read(buf).await {
                Ok(Some(n)) => Ok(n),
                Ok(None) => Ok(0), // Stream finished
                Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            }
        })
    }
}

impl Write for QuicBidiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut send = self.send.lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        self.runtime.block_on(async {
            send.write(buf)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok(buf.len())
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Read/Write for &QuicBidiStream (needed for Arc<FlezhaStream>)
impl Read for &QuicBidiStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut recv = self.recv.lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        self.runtime.block_on(async {
            match recv.read(buf).await {
                Ok(Some(n)) => Ok(n),
                Ok(None) => Ok(0),
                Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            }
        })
    }
}

impl Write for &QuicBidiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut send = self.send.lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?;

        self.runtime.block_on(async {
            send.write(buf)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            Ok(buf.len())
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
