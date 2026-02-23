use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use crossbeam::channel;
use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token, Waker};
use quinn_proto::{
    ClientConfig, ConnectionHandle, DatagramEvent, Dir, EndpointConfig, Event, StreamEvent,
    StreamId, Transmit,
};

const UDP_TOKEN: Token = Token(0);
const WAKER_TOKEN: Token = Token(1);

// ---------------------------------------------------------------------------
// Driver ↔ App communication types
// ---------------------------------------------------------------------------

enum DriverCommand {
    OpenStream {
        read_tx: channel::Sender<Bytes>,
        reply: channel::Sender<io::Result<StreamId>>,
    },
    Write {
        stream_id: StreamId,
        data: Vec<u8>,
        reply: channel::Sender<io::Result<usize>>,
    },
    Shutdown,
}

struct StreamSlot {
    read_tx: channel::Sender<Bytes>,
    write_buf: Vec<u8>,
    pending_write_reply: Option<(usize, channel::Sender<io::Result<usize>>)>,
}

// ---------------------------------------------------------------------------
// ChannelReader — sync Read over crossbeam channel
// ---------------------------------------------------------------------------

struct ChannelReader {
    rx: channel::Receiver<Bytes>,
    buf: Bytes,
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        // Serve from buffered chunk if available
        if !self.buf.is_empty() {
            let n = std::cmp::min(out.len(), self.buf.len());
            out[..n].copy_from_slice(&self.buf[..n]);
            self.buf = self.buf.slice(n..);
            return Ok(n);
        }

        // Buffer exhausted — wait for next chunk from driver
        match self.rx.recv() {
            Ok(chunk) => {
                let n = std::cmp::min(out.len(), chunk.len());
                out[..n].copy_from_slice(&chunk[..n]);
                if n < chunk.len() {
                    self.buf = chunk.slice(n..);
                }
                Ok(n)
            }
            Err(_) => Ok(0), // Channel closed = stream finished
        }
    }
}

// ---------------------------------------------------------------------------
// QuicClient — public API
// ---------------------------------------------------------------------------

/// Holds a QUIC connection driven by a background I/O thread using quinn-proto.
pub struct QuicClient {
    cmd_tx: channel::Sender<DriverCommand>,
    waker: Arc<Waker>,
    _driver_handle: thread::JoinHandle<()>,
}

impl QuicClient {
    /// Connect to a QUIC server at the given address using the CA cert for verification.
    pub fn connect(address: &str, ca_cert_path: &str) -> io::Result<QuicClient> {
        // --- TLS config ---
        let ca_cert_pem = std::fs::read(ca_cert_path)?;
        let mut reader = std::io::BufReader::new(&ca_cert_pem[..]);

        let mut root_store = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut reader) {
            let cert = cert.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            root_store
                .add(cert)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }

        let mut crypto = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        crypto.alpn_protocols = vec![b"flezha".to_vec()];

        let mut client_config = ClientConfig::new(Arc::new(
            quinn_proto::crypto::rustls::QuicClientConfig::try_from(crypto)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?,
        ));

        let mut transport = quinn_proto::TransportConfig::default();
        transport.keep_alive_interval(Some(Duration::from_secs(5)));
        transport.stream_receive_window(8_388_608u32.into());
        transport.receive_window(16_777_216u32.into());
        transport.send_window(8_388_608);
        client_config.transport_config(Arc::new(transport));

        // --- UDP socket + mio Poll ---
        let addr: SocketAddr = address
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid address: {}", e)))?;

        let std_udp = std::net::UdpSocket::bind("0.0.0.0:0")?;
        std_udp.set_nonblocking(true)?;
        let mut udp = UdpSocket::from_std(std_udp);

        let mut poll = Poll::new()?;
        poll.registry()
            .register(&mut udp, UDP_TOKEN, Interest::READABLE | Interest::WRITABLE)?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKER_TOKEN)?);

        // --- quinn-proto Endpoint + Connection ---
        let mut endpoint =
            quinn_proto::Endpoint::new(Arc::new(EndpointConfig::default()), None, true, None);

        let (conn_handle, mut connection) = endpoint
            .connect(Instant::now(), client_config, addr, "localhost")
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("QUIC connect error: {}", e)))?;

        // --- Blocking handshake ---
        drive_handshake(&mut poll, &udp, &mut endpoint, conn_handle, &mut connection)?;

        // --- Spawn driver thread ---
        let (cmd_tx, cmd_rx) = channel::unbounded::<DriverCommand>();
        let waker_clone = Arc::clone(&waker);

        let driver_handle = thread::Builder::new()
            .name("quic-io-driver".into())
            .spawn(move || {
                driver_loop(poll, udp, endpoint, conn_handle, connection, cmd_rx);
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(QuicClient {
            cmd_tx,
            waker: waker_clone,
            _driver_handle: driver_handle,
        })
    }

    /// Open a new bidirectional stream on this connection.
    pub fn open_bidi_stream(&self) -> io::Result<QuicBidiStream> {
        let (read_tx, read_rx) = channel::bounded::<Bytes>(64);
        let (reply_tx, reply_rx) = channel::bounded::<io::Result<StreamId>>(1);

        self.cmd_tx
            .send(DriverCommand::OpenStream {
                read_tx,
                reply: reply_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Driver thread gone"))?;
        self.waker.wake().map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let stream_id = reply_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Driver reply dropped"))??;

        Ok(QuicBidiStream {
            stream_id,
            cmd_tx: self.cmd_tx.clone(),
            waker: Arc::clone(&self.waker),
            read_rx: Arc::new(Mutex::new(ChannelReader {
                rx: read_rx,
                buf: Bytes::new(),
            })),
        })
    }
}

impl Drop for QuicClient {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(DriverCommand::Shutdown);
        let _ = self.waker.wake();
    }
}

// ---------------------------------------------------------------------------
// QuicBidiStream — public API, same interface as before
// ---------------------------------------------------------------------------

/// A bidirectional QUIC stream that implements std::io::Read and Write.
///
/// All QUIC I/O is driven by a background driver thread using quinn-proto.
/// App threads communicate with the driver via crossbeam channels.
pub struct QuicBidiStream {
    stream_id: StreamId,
    cmd_tx: channel::Sender<DriverCommand>,
    waker: Arc<Waker>,
    read_rx: Arc<Mutex<ChannelReader>>,
}

impl QuicBidiStream {
    /// Clone by sharing the write channel and read channel.
    pub fn try_clone(&self) -> QuicBidiStream {
        QuicBidiStream {
            stream_id: self.stream_id,
            cmd_tx: self.cmd_tx.clone(),
            waker: Arc::clone(&self.waker),
            read_rx: Arc::clone(&self.read_rx),
        }
    }
}

impl Read for QuicBidiStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_rx
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?
            .read(buf)
    }
}

impl Write for QuicBidiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (reply_tx, reply_rx) = channel::bounded(1);
        self.cmd_tx
            .send(DriverCommand::Write {
                stream_id: self.stream_id,
                data: buf.to_vec(),
                reply: reply_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "QUIC write channel closed"))?;
        self.waker
            .wake()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Driver reply dropped"))?
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// Read/Write for &QuicBidiStream (needed for Arc<FlezhaStream>)
impl Read for &QuicBidiStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_rx
            .lock()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Lock poisoned: {}", e)))?
            .read(buf)
    }
}

impl Write for &QuicBidiStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let (reply_tx, reply_rx) = channel::bounded(1);
        self.cmd_tx
            .send(DriverCommand::Write {
                stream_id: self.stream_id,
                data: buf.to_vec(),
                reply: reply_tx,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "QUIC write channel closed"))?;
        self.waker
            .wake()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        reply_rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Driver reply dropped"))?
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Handshake — blocking mio loop until Event::Connected
// ---------------------------------------------------------------------------

fn drive_handshake(
    poll: &mut Poll,
    udp: &UdpSocket,
    endpoint: &mut quinn_proto::Endpoint,
    conn_handle: ConnectionHandle,
    connection: &mut quinn_proto::Connection,
) -> io::Result<()> {
    let mut events = Events::with_capacity(64);
    let mut udp_buf = vec![0u8; 65536];
    let mut transmit_buf = Vec::new();

    // Flush initial handshake packets
    flush_transmits(connection, udp, &mut transmit_buf)?;

    loop {
        let timeout = connection.poll_timeout().map(|deadline| {
            deadline.saturating_duration_since(Instant::now())
        });
        let poll_timeout = timeout.unwrap_or(Duration::from_millis(100));
        poll.poll(&mut events, Some(poll_timeout))?;

        let now = Instant::now();

        // Handle timer
        if let Some(deadline) = connection.poll_timeout() {
            if now >= deadline {
                connection.handle_timeout(now);
            }
        }

        // Read incoming UDP
        loop {
            match udp.recv_from(&mut udp_buf) {
                Ok((n, remote)) => {
                    let data = BytesMut::from(&udp_buf[..n]);
                    let mut resp_buf = Vec::new();
                    if let Some(event) = endpoint.handle(now, remote, None, None, data, &mut resp_buf) {
                        match event {
                            DatagramEvent::ConnectionEvent(ch, event) => {
                                if ch == conn_handle {
                                    connection.handle_event(event);
                                }
                            }
                            DatagramEvent::Response(transmit) => {
                                send_transmit(udp, &transmit, &resp_buf)?;
                            }
                            DatagramEvent::NewConnection(_) => {}
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e),
            }
        }

        // Pump endpoint ↔ connection event feedback
        pump_endpoint_events(endpoint, conn_handle, connection);

        // Flush outgoing packets
        flush_transmits(connection, udp, &mut transmit_buf)?;

        // Check for Connected event
        while let Some(event) = connection.poll() {
            match event {
                Event::Connected => return Ok(()),
                Event::ConnectionLost { reason } => {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        format!("QUIC handshake failed: {}", reason),
                    ));
                }
                _ => {}
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Driver loop — main I/O thread
// ---------------------------------------------------------------------------

fn driver_loop(
    mut poll: Poll,
    udp: UdpSocket,
    mut endpoint: quinn_proto::Endpoint,
    conn_handle: ConnectionHandle,
    mut connection: quinn_proto::Connection,
    cmd_rx: channel::Receiver<DriverCommand>,
) {
    let mut events = Events::with_capacity(256);
    let mut udp_buf = vec![0u8; 65536];
    let mut transmit_buf = Vec::new();
    let mut streams: HashMap<StreamId, StreamSlot> = HashMap::new();

    loop {
        // 1. Poll with timeout from connection
        let timeout = connection
            .poll_timeout()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()));
        let poll_timeout = timeout.unwrap_or(Duration::from_millis(100));
        let _ = poll.poll(&mut events, Some(poll_timeout));

        let now = Instant::now();

        // 2. Handle timer
        if let Some(deadline) = connection.poll_timeout() {
            if now >= deadline {
                connection.handle_timeout(now);
            }
        }

        // 3. Read incoming UDP datagrams
        loop {
            match udp.recv_from(&mut udp_buf) {
                Ok((n, remote)) => {
                    let data = BytesMut::from(&udp_buf[..n]);
                    let mut resp_buf = Vec::new();
                    if let Some(event) =
                        endpoint.handle(now, remote, None, None, data, &mut resp_buf)
                    {
                        match event {
                            DatagramEvent::ConnectionEvent(ch, event) => {
                                if ch == conn_handle {
                                    connection.handle_event(event);
                                }
                            }
                            DatagramEvent::Response(transmit) => {
                                let _ = send_transmit(&udp, &transmit, &resp_buf);
                            }
                            DatagramEvent::NewConnection(_) => {}
                        }
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }

        // 4. Process commands from app threads
        while let Ok(cmd) = cmd_rx.try_recv() {
            match cmd {
                DriverCommand::OpenStream { read_tx, reply } => {
                    match connection.streams().open(Dir::Bi) {
                        Some(stream_id) => {
                            streams.insert(
                                stream_id,
                                StreamSlot {
                                    read_tx,
                                    write_buf: Vec::new(),
                                    pending_write_reply: None,
                                },
                            );
                            let _ = reply.send(Ok(stream_id));
                        }
                        None => {
                            let _ = reply.send(Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Stream limit reached",
                            )));
                        }
                    }
                }
                DriverCommand::Write {
                    stream_id,
                    data,
                    reply,
                } => {
                    if let Some(slot) = streams.get_mut(&stream_id) {
                        let total_len = data.len();
                        match connection.send_stream(stream_id).write(&data) {
                            Ok(written) => {
                                if written < total_len {
                                    // Partial write — buffer remainder, defer reply
                                    slot.write_buf.extend_from_slice(&data[written..]);
                                    slot.pending_write_reply = Some((total_len, reply));
                                } else {
                                    let _ = reply.send(Ok(total_len));
                                }
                            }
                            Err(quinn_proto::WriteError::Blocked) => {
                                // Fully blocked — buffer everything, defer reply
                                slot.write_buf = data;
                                slot.pending_write_reply = Some((total_len, reply));
                            }
                            Err(e) => {
                                let _ = reply.send(Err(io::Error::new(
                                    io::ErrorKind::BrokenPipe,
                                    format!("QUIC write error: {}", e),
                                )));
                            }
                        }
                    } else {
                        let _ = reply.send(Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "Unknown stream ID",
                        )));
                    }
                }
                DriverCommand::Shutdown => {
                    connection.close(
                        now,
                        quinn_proto::VarInt::from_u32(0),
                        b"shutdown".to_vec().into(),
                    );
                    let _ = flush_transmits(&mut connection, &udp, &mut transmit_buf);
                    return;
                }
            }
        }

        // 5. Pump endpoint ↔ connection event feedback
        pump_endpoint_events(&mut endpoint, conn_handle, &mut connection);

        // 6. Poll connection events — dispatch stream events
        while let Some(event) = connection.poll() {
            match event {
                Event::Stream(StreamEvent::Readable { id }) => {
                    drain_readable(&mut connection, id, &mut streams);
                }
                Event::Stream(StreamEvent::Writable { id }) => {
                    flush_write_buffer(&mut connection, id, &mut streams);
                }
                Event::ConnectionLost { .. } => {
                    return;
                }
                _ => {}
            }
        }

        // 7. Flush outgoing packets
        let _ = flush_transmits(&mut connection, &udp, &mut transmit_buf);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Drain readable data from a stream and push to the channel.
fn drain_readable(
    connection: &mut quinn_proto::Connection,
    id: StreamId,
    streams: &mut HashMap<StreamId, StreamSlot>,
) {
    let slot = match streams.get(&id) {
        Some(s) => s,
        None => return,
    };
    let read_tx = slot.read_tx.clone();

    match connection.recv_stream(id).read(true) {
        Ok(mut chunks) => {
            loop {
                match chunks.next(usize::MAX) {
                    Ok(Some(chunk)) => {
                        if read_tx.send(chunk.bytes).is_err() {
                            break; // Consumer dropped
                        }
                    }
                    Ok(None) => break, // No more data
                    Err(quinn_proto::ReadError::Blocked) => break,
                    Err(_) => break,
                }
            }
            let _ = chunks.finalize();
        }
        Err(_) => {}
    }
}

/// Flush buffered write data for a stream that became writable.
fn flush_write_buffer(
    connection: &mut quinn_proto::Connection,
    id: StreamId,
    streams: &mut HashMap<StreamId, StreamSlot>,
) {
    let slot = match streams.get_mut(&id) {
        Some(s) => s,
        None => return,
    };

    if slot.write_buf.is_empty() {
        return;
    }

    match connection.send_stream(id).write(&slot.write_buf) {
        Ok(written) => {
            slot.write_buf.drain(..written);
            if slot.write_buf.is_empty() {
                // All buffered data sent — reply to the app
                if let Some((total, reply)) = slot.pending_write_reply.take() {
                    let _ = reply.send(Ok(total));
                }
            }
        }
        Err(quinn_proto::WriteError::Blocked) => {
            // Still blocked, keep buffer
        }
        Err(e) => {
            slot.write_buf.clear();
            if let Some((_, reply)) = slot.pending_write_reply.take() {
                let _ = reply.send(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("QUIC write error: {}", e),
                )));
            }
        }
    }
}

/// Pump endpoint events back and forth between endpoint and connection.
fn pump_endpoint_events(
    endpoint: &mut quinn_proto::Endpoint,
    conn_handle: ConnectionHandle,
    connection: &mut quinn_proto::Connection,
) {
    while let Some(endpoint_event) = connection.poll_endpoint_events() {
        if let Some(conn_event) = endpoint.handle_event(conn_handle, endpoint_event) {
            connection.handle_event(conn_event);
        }
    }
}

/// Flush all pending transmits from the connection to the UDP socket.
fn flush_transmits(
    connection: &mut quinn_proto::Connection,
    udp: &UdpSocket,
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    let now = Instant::now();
    while let Some(transmit) = connection.poll_transmit(now, 1, buf) {
        send_transmit(udp, &transmit, buf)?;
        buf.clear();
    }
    Ok(())
}

/// Send a single transmit (datagram) on the UDP socket.
fn send_transmit(udp: &UdpSocket, transmit: &Transmit, buf: &[u8]) -> io::Result<()> {
    let data = &buf[..transmit.size];
    match udp.send_to(data, transmit.destination) {
        Ok(_) => Ok(()),
        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(()),
        Err(e) => Err(e),
    }
}
