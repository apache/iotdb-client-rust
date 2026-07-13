// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Low-level Thrift connection to an IoTDB node.
//!
//! Mirrors `src/connection/Connection.ts` (Node.js) and the Thrift client setup
//! in the C# SDK: TCP (optionally TLS) → TFramedTransport → TBinaryProtocol
//! (or TCompactProtocol when RPC compression is enabled) → IClientRPCService
//! client.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol,
    TInputProtocol, TOutputProtocol,
};
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel};

use crate::error::{Error, Result};
use crate::protocol::client::IClientRPCServiceSyncClient;

/// Default IoTDB DataNode RPC port.
pub const DEFAULT_PORT: u16 = 6667;

/// Wire protocol used on top of the framed transport.
///
/// IoTDB's Thrift server speaks **one** protocol per server instance,
/// chosen by the server config `dn_rpc_thrift_compression_enable`
/// (default `false` → binary). There is **no** per-connection
/// auto-detection: a compact-protocol client against a binary-protocol
/// server fails at the first RPC, and vice versa — pick the protocol that
/// matches the server (the C# SDK's `enableRpcCompression` does the same
/// blind switch).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RpcProtocol {
    /// Strict TBinaryProtocol — matches the server default.
    #[default]
    Binary,
    /// TCompactProtocol — matches `dn_rpc_thrift_compression_enable=true`
    /// ("RPC compression" in IoTDB terms is the compact protocol, not a
    /// compressed stream).
    Compact,
}

/// TLS settings for a [`Connection`] (cargo feature `tls`).
#[cfg(feature = "tls")]
#[derive(Debug, Clone, Default)]
pub struct TlsOptions {
    /// PEM certificate added as a trusted root (e.g. a private CA or the
    /// server's self-signed certificate).
    pub ca_cert_path: Option<std::path::PathBuf>,
    /// Skip certificate verification entirely (self-signed test certs).
    /// **Dangerous** outside tests. Default `false`.
    pub accept_invalid_certs: bool,
    /// Hostname used for SNI + certificate validation instead of the
    /// endpoint host (e.g. when connecting by IP).
    pub domain_override: Option<String>,
    /// PEM client certificate for mutual TLS (server has
    /// `thrift_ssl_client_auth=true`). Must be set together with
    /// [`client_key_path`](Self::client_key_path); mirrors the Node.js
    /// `sslOptions.cert`.
    pub client_cert_path: Option<std::path::PathBuf>,
    /// PEM PKCS#8 private key for the client certificate. Must be set
    /// together with [`client_cert_path`](Self::client_cert_path); mirrors
    /// the Node.js `sslOptions.key`.
    pub client_key_path: Option<std::path::PathBuf>,
}

/// How to open a [`Connection`]: timeout, wire protocol, optional TLS.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    /// TCP connect timeout per endpoint attempt. Default 10 s.
    pub connect_timeout: Duration,
    /// Wire protocol; must match the server (see [`RpcProtocol`]).
    pub protocol: RpcProtocol,
    /// Wrap the TCP stream in TLS before the Thrift transports.
    #[cfg(feature = "tls")]
    pub tls: Option<TlsOptions>,
}

impl Default for ConnectionOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            protocol: RpcProtocol::Binary,
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

/// Type-erased Thrift input protocol (binary or compact, plain or TLS).
pub type BoxedInputProtocol = Box<dyn TInputProtocol + Send>;
/// Type-erased Thrift output protocol (binary or compact, plain or TLS).
pub type BoxedOutputProtocol = Box<dyn TOutputProtocol + Send>;

/// The generated RPC client over framed transport and a type-erased
/// protocol pair (the `thrift` crate forwards the protocol traits through
/// `Box`, so the generated generic client works unchanged).
pub type RpcClient = IClientRPCServiceSyncClient<BoxedInputProtocol, BoxedOutputProtocol>;

/// A single endpoint `host:port` of an IoTDB DataNode (default port 6667).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Parse a `"host:port"` node-url string.
    ///
    /// Splits on the **last** `:` so IPv6 literals work; surrounding `[]`
    /// brackets on the host part are stripped (e.g. `"[::1]:6667"` → host `::1`).
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        let idx = s
            .rfind(':')
            .ok_or_else(|| Error::Client(format!("invalid node url '{s}': expected host:port")))?;
        let (host_part, port_part) = (&s[..idx], &s[idx + 1..]);
        let port: u16 = port_part.parse().map_err(|_| {
            Error::Client(format!("invalid node url '{s}': bad port '{port_part}'"))
        })?;
        let host = host_part
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(host_part);
        if host.is_empty() {
            return Err(Error::Client(format!("invalid node url '{s}': empty host")));
        }
        Ok(Self::new(host, port))
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// Low-level connection wrapper. Owns the Thrift transport/protocol pair
/// and the generated `IClientRPCService` client.
pub struct Connection {
    endpoint: Endpoint,
    protocol: RpcProtocol,
    client: RpcClient,
}

impl Connection {
    /// Establish a TCP connection to `endpoint` (bounded by
    /// `options.connect_timeout`), optionally wrap it in TLS, and stack
    /// framed transport + the selected protocol on top.
    pub fn open(endpoint: Endpoint, options: &ConnectionOptions) -> Result<Self> {
        let stream = connect_stream(&endpoint, options.connect_timeout)?;
        stream.set_nodelay(true).map_err(thrift::Error::from)?;

        #[cfg(feature = "tls")]
        if let Some(tls) = &options.tls {
            let stream = tls_handshake(&endpoint, stream, tls)?;
            let shared = SharedTlsStream::new(stream);
            let (input, output) = build_protocols(shared.clone(), shared, options.protocol);
            return Ok(Self {
                endpoint,
                protocol: options.protocol,
                client: IClientRPCServiceSyncClient::new(input, output),
            });
        }

        let channel = TTcpChannel::with_stream(stream);
        let (read_half, write_half) = channel.split()?;
        let (input, output) = build_protocols(read_half, write_half, options.protocol);
        Ok(Self {
            endpoint,
            protocol: options.protocol,
            client: IClientRPCServiceSyncClient::new(input, output),
        })
    }

    /// Mutable access to the generated RPC client for issuing calls.
    pub fn client_mut(&mut self) -> &mut RpcClient {
        &mut self.client
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The wire protocol this connection was opened with.
    pub fn protocol(&self) -> RpcProtocol {
        self.protocol
    }
}

/// Stack framed transport + the selected protocol over a read/write pair
/// and type-erase the result (see [`RpcClient`]).
fn build_protocols<R, W>(
    read: R,
    write: W,
    protocol: RpcProtocol,
) -> (BoxedInputProtocol, BoxedOutputProtocol)
where
    R: Read + Send + 'static,
    W: Write + Send + 'static,
{
    let read_transport = TFramedReadTransport::new(read);
    let write_transport = TFramedWriteTransport::new(write);
    match protocol {
        RpcProtocol::Binary => (
            Box::new(TBinaryInputProtocol::new(read_transport, true)),
            Box::new(TBinaryOutputProtocol::new(write_transport, true)),
        ),
        RpcProtocol::Compact => (
            Box::new(TCompactInputProtocol::new(read_transport)),
            Box::new(TCompactOutputProtocol::new(write_transport)),
        ),
    }
}

/// Resolve the endpoint and try each resolved address with the connect timeout.
fn connect_stream(endpoint: &Endpoint, connect_timeout: Duration) -> Result<TcpStream> {
    let addrs = (endpoint.host.as_str(), endpoint.port)
        .to_socket_addrs()
        .map_err(thrift::Error::from)?;
    let mut last_err: Option<std::io::Error> = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, connect_timeout) {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }
    Err(match last_err {
        Some(e) => Error::Thrift(thrift::Error::from(e)),
        None => Error::Client(format!("could not resolve endpoint {endpoint}")),
    })
}

/// Run the TLS handshake over an established TCP stream.
#[cfg(feature = "tls")]
fn tls_handshake(
    endpoint: &Endpoint,
    stream: TcpStream,
    tls: &TlsOptions,
) -> Result<native_tls::TlsStream<TcpStream>> {
    let mut builder = native_tls::TlsConnector::builder();
    if let Some(path) = &tls.ca_cert_path {
        let pem = std::fs::read(path).map_err(|e| {
            Error::Client(format!(
                "cannot read CA certificate {}: {e}",
                path.display()
            ))
        })?;
        let cert = native_tls::Certificate::from_pem(&pem).map_err(Error::Tls)?;
        builder.add_root_certificate(cert);
    }
    if tls.accept_invalid_certs {
        builder.danger_accept_invalid_certs(true);
    }
    match (&tls.client_cert_path, &tls.client_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let cert = std::fs::read(cert_path).map_err(|e| {
                Error::Client(format!(
                    "cannot read client certificate {}: {e}",
                    cert_path.display()
                ))
            })?;
            let key = std::fs::read(key_path).map_err(|e| {
                Error::Client(format!(
                    "cannot read client key {}: {e}",
                    key_path.display()
                ))
            })?;
            let identity = native_tls::Identity::from_pkcs8(&cert, &key).map_err(Error::Tls)?;
            builder.identity(identity);
        }
        (None, None) => {}
        _ => {
            return Err(Error::Client(
                "mutual TLS requires both client_cert_path and client_key_path".into(),
            ))
        }
    }
    let connector = builder.build().map_err(Error::Tls)?;
    let domain = tls.domain_override.as_deref().unwrap_or(&endpoint.host);
    connector.connect(domain, stream).map_err(|e| match e {
        native_tls::HandshakeError::Failure(e) => Error::Tls(e),
        // Blocking sockets never yield the mid-handshake variant.
        native_tls::HandshakeError::WouldBlock(_) => {
            Error::Client("TLS handshake interrupted".into())
        }
    })
}

/// A `TlsStream` shared between the read and write transports.
///
/// `TTcpChannel::split` clones the underlying OS socket, but a TLS stream
/// cannot be split that way (record layer state is shared), so both framed
/// transports hold the same stream behind a mutex. The generated sync
/// client fully writes + flushes a request before reading the response, so
/// read and write never contend.
#[cfg(feature = "tls")]
#[derive(Clone)]
struct SharedTlsStream(std::sync::Arc<std::sync::Mutex<native_tls::TlsStream<TcpStream>>>);

#[cfg(feature = "tls")]
impl SharedTlsStream {
    fn new(stream: native_tls::TlsStream<TcpStream>) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(stream)))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, native_tls::TlsStream<TcpStream>> {
        self.0.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[cfg(feature = "tls")]
impl Read for SharedTlsStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.lock().read(buf)
    }
}

#[cfg(feature = "tls")]
impl Write for SharedTlsStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.lock().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.lock().flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ipv4() {
        let ep = Endpoint::parse("127.0.0.1:6667").unwrap();
        assert_eq!(ep, Endpoint::new("127.0.0.1", 6667));
    }

    #[test]
    fn parse_hostname() {
        let ep = Endpoint::parse("iotdb.example.com:1234").unwrap();
        assert_eq!(ep, Endpoint::new("iotdb.example.com", 1234));
    }

    #[test]
    fn parse_ipv6_bracketed() {
        let ep = Endpoint::parse("[::1]:6667").unwrap();
        assert_eq!(ep, Endpoint::new("::1", 6667));

        let ep = Endpoint::parse("[2001:db8::1]:6668").unwrap();
        assert_eq!(ep, Endpoint::new("2001:db8::1", 6668));
    }

    #[test]
    fn parse_trims_whitespace() {
        let ep = Endpoint::parse("  localhost:6667 ").unwrap();
        assert_eq!(ep, Endpoint::new("localhost", 6667));
    }

    #[test]
    fn parse_no_port_is_error() {
        assert!(Endpoint::parse("localhost").is_err());
    }

    #[test]
    fn parse_bad_port_is_error() {
        assert!(Endpoint::parse("localhost:abc").is_err());
        assert!(Endpoint::parse("localhost:99999").is_err());
        assert!(Endpoint::parse("localhost:").is_err());
    }

    #[test]
    fn parse_empty_host_is_error() {
        assert!(Endpoint::parse(":6667").is_err());
        assert!(Endpoint::parse("[]:6667").is_err());
    }

    #[test]
    fn display_roundtrip() {
        assert_eq!(
            Endpoint::new("localhost", 6667).to_string(),
            "localhost:6667"
        );
        assert_eq!(Endpoint::new("::1", 6667).to_string(), "[::1]:6667");
        assert_eq!(
            Endpoint::parse(&Endpoint::new("::1", 6667).to_string()).unwrap(),
            Endpoint::new("::1", 6667)
        );
    }

    #[test]
    fn default_options_are_binary_no_tls() {
        let options = ConnectionOptions::default();
        assert_eq!(options.connect_timeout, Duration::from_secs(10));
        assert_eq!(options.protocol, RpcProtocol::Binary);
        #[cfg(feature = "tls")]
        assert!(options.tls.is_none());
    }

    /// A local listener that accepts and immediately drops connections, so
    /// `Connection::open` (which issues no RPC) succeeds for any protocol.
    pub(super) fn accept_then_drop_listener() -> Endpoint {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("local_addr").port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(s) => drop(s),
                    Err(_) => break,
                }
            }
        });
        Endpoint::new("127.0.0.1", port)
    }

    /// A local listener that accepts one connection, reports the first byte
    /// the client puts on the wire, then closes (unblocking the client with
    /// an EOF/reset). Lets tests assert *which* stack touched the socket:
    /// a TLS ClientHello starts with the handshake record type `0x16`, a
    /// plain Thrift framed message with the frame-length MSB `0x00`.
    pub(super) fn first_byte_listener() -> (Endpoint, std::sync::mpsc::Receiver<u8>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("local_addr").port();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut byte = [0u8; 1];
                if stream.read_exact(&mut byte).is_ok() {
                    let _ = tx.send(byte[0]);
                }
            }
        });
        (Endpoint::new("127.0.0.1", port), rx)
    }

    /// Dispatch (Node.js `Connection.test.ts` analogue): without TLS the
    /// plain TCP stack talks to the socket — the first wire byte of an RPC
    /// is the framed-transport length MSB (`0x00`), not a TLS ClientHello
    /// record type (`0x16`).
    #[test]
    fn plain_dispatch_writes_thrift_frame_not_tls() {
        use crate::protocol::client::TIClientRPCServiceSyncClient;

        let (endpoint, first_byte) = first_byte_listener();
        #[allow(clippy::needless_update)]
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            ..Default::default()
        };
        let mut connection = Connection::open(endpoint, &options).expect("plain open");
        // The RPC itself fails once the listener closes after one byte —
        // only the bytes it managed to put on the wire matter here.
        let _ = connection.client_mut().request_statement_id(1);
        let byte = first_byte
            .recv_timeout(Duration::from_secs(5))
            .expect("first wire byte");
        assert_eq!(byte, 0x00, "expected framed length MSB, got 0x{byte:02x}");
    }

    /// Both protocol variants construct their transport/protocol stack and
    /// report the choice back via `Connection::protocol`. (Wire-level
    /// verification against a live server lives in the session tests.)
    #[test]
    fn open_with_each_protocol() {
        let endpoint = accept_then_drop_listener();
        for protocol in [RpcProtocol::Binary, RpcProtocol::Compact] {
            // The struct update is only "needless" without the tls feature,
            // which adds a field this literal doesn't name.
            #[allow(clippy::needless_update)]
            let options = ConnectionOptions {
                connect_timeout: Duration::from_millis(500),
                protocol,
                ..Default::default()
            };
            let connection = Connection::open(endpoint.clone(), &options).expect("open");
            assert_eq!(connection.protocol(), protocol);
            assert_eq!(connection.endpoint(), &endpoint);
        }
    }
}

#[cfg(all(test, feature = "tls"))]
mod tls_tests {
    use super::*;
    use crate::protocol::client::TIClientRPCServiceSyncClient;
    use std::path::PathBuf;

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/tls")
            .join(name)
    }

    /// Spawn a TLS acceptor on a loopback port that completes one handshake
    /// and then drops the connection. Uses the checked-in self-signed cert
    /// (CN=localhost, SAN DNS:localhost + IP:127.0.0.1, 100-year validity).
    fn tls_acceptor_once() -> Endpoint {
        let cert = std::fs::read(fixture("cert.pem")).expect("read cert fixture");
        let key = std::fs::read(fixture("key.pem")).expect("read key fixture");
        let identity = native_tls::Identity::from_pkcs8(&cert, &key).expect("identity");
        let acceptor = native_tls::TlsAcceptor::new(identity).expect("acceptor");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("local_addr").port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    // Handshake (which may itself fail when the client
                    // rejects our cert — fine, just move on), then drop.
                    Ok(s) => drop(acceptor.accept(s)),
                    Err(_) => break,
                }
            }
        });
        Endpoint::new("127.0.0.1", port)
    }

    /// Full client-side TLS path with the fixture cert as trusted root and
    /// hostname pinned via `domain_override`: the handshake completes (so
    /// `Connection::open` succeeds), and the first RPC then dies at the
    /// Thrift layer because the acceptor closed the connection — proving
    /// the failure is post-handshake.
    #[test]
    fn tls_handshake_with_trusted_root_then_thrift_failure() {
        let endpoint = tls_acceptor_once();
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            tls: Some(TlsOptions {
                ca_cert_path: Some(fixture("cert.pem")),
                accept_invalid_certs: false,
                domain_override: Some("localhost".into()),
                ..Default::default()
            }),
        };
        let mut connection = Connection::open(endpoint, &options).expect("TLS handshake");
        assert_eq!(connection.protocol(), RpcProtocol::Binary);
        let err = connection
            .client_mut()
            .request_statement_id(1)
            .expect_err("RPC on a closed TLS connection must fail");
        // Post-handshake: the error is a Thrift transport error, not TLS.
        let msg = err.to_string();
        assert!(!msg.to_lowercase().contains("certificate"), "got: {msg}");
    }

    /// Without the fixture as trusted root, certificate validation rejects
    /// the self-signed server during the handshake: `Connection::open`
    /// itself fails with a TLS error.
    #[test]
    fn tls_untrusted_cert_fails_handshake() {
        let endpoint = tls_acceptor_once();
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            tls: Some(TlsOptions {
                ca_cert_path: None,
                accept_invalid_certs: false,
                domain_override: Some("localhost".into()),
                ..Default::default()
            }),
        };
        let err = match Connection::open(endpoint, &options) {
            Ok(_) => panic!("untrusted self-signed cert must fail the handshake"),
            Err(e) => e,
        };
        assert!(matches!(err, Error::Tls(_)), "got {err:?}");
    }

    /// `accept_invalid_certs` bypasses validation for self-signed test
    /// certs — handshake succeeds without any trusted root.
    #[test]
    fn tls_accept_invalid_certs_bypasses_validation() {
        let endpoint = tls_acceptor_once();
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Compact, // also exercise compact-over-TLS
            tls: Some(TlsOptions {
                accept_invalid_certs: true,
                ..Default::default()
            }),
        };
        let connection = Connection::open(endpoint, &options).expect("TLS handshake");
        assert_eq!(connection.protocol(), RpcProtocol::Compact);
    }

    /// Dispatch (Node.js `Connection.test.ts` analogue), TLS side: with
    /// `tls: Some(..)` the first wire byte is the TLS handshake record type
    /// `0x16` (ClientHello) — the plain Thrift stack never touches the
    /// socket. The listener is not a TLS server, so `open` itself fails.
    #[test]
    fn tls_dispatch_sends_client_hello() {
        let (endpoint, first_byte) = super::tests::first_byte_listener();
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            tls: Some(TlsOptions {
                accept_invalid_certs: true,
                ..Default::default()
            }),
        };
        assert!(
            Connection::open(endpoint, &options).is_err(),
            "plain listener is not a TLS server"
        );
        let byte = first_byte
            .recv_timeout(Duration::from_secs(5))
            .expect("first wire byte");
        assert_eq!(
            byte, 0x16,
            "expected TLS handshake record type, got 0x{byte:02x}"
        );
    }

    /// Dispatch pair against the *same kind* of plain (non-TLS) endpoint:
    /// `tls: None` opens fine (plain TCP), `tls: Some(..)` — even with
    /// certificate verification disabled — dies in the handshake with a
    /// TLS error. Together with the ClientHello byte check this proves the
    /// `tls` option selects the code path, mirroring the Node.js test that
    /// asserts the SSL constructor is (not) called.
    #[test]
    fn tls_option_selects_stack_against_plain_endpoint() {
        let endpoint = super::tests::accept_then_drop_listener();

        let plain = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            tls: None,
        };
        Connection::open(endpoint.clone(), &plain).expect("plain open against plain listener");

        let tls = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            tls: Some(TlsOptions {
                accept_invalid_certs: true,
                ..Default::default()
            }),
        };
        let err = match Connection::open(endpoint, &tls) {
            Ok(_) => panic!("TLS handshake against a plain endpoint must fail"),
            Err(e) => e,
        };
        assert!(matches!(err, Error::Tls(_)), "got {err:?}");
    }

    /// Mutual TLS: a PEM client certificate + PKCS#8 key load into an
    /// identity and the handshake completes with the identity configured.
    /// `native-tls`'s `TlsAcceptor` has no API to *request* a client
    /// certificate, so the loopback server cannot verify it — acceptor-side
    /// client-auth is exercised only by the live `IOTDB_TLS_URL` test
    /// against a real server.
    #[test]
    fn tls_client_identity_handshake_succeeds() {
        let endpoint = tls_acceptor_once();
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            tls: Some(TlsOptions {
                ca_cert_path: Some(fixture("cert.pem")),
                domain_override: Some("localhost".into()),
                client_cert_path: Some(fixture("client-cert.pem")),
                client_key_path: Some(fixture("client-key.pem")),
                ..Default::default()
            }),
        };
        let connection = Connection::open(endpoint, &options).expect("TLS handshake with identity");
        assert_eq!(connection.protocol(), RpcProtocol::Binary);
    }

    /// Setting only one of the client cert/key pair is a config error
    /// caught before any I/O.
    #[test]
    fn tls_client_identity_requires_both_paths() {
        let endpoint = tls_acceptor_once();
        for (cert, key) in [
            (Some(fixture("client-cert.pem")), None),
            (None, Some(fixture("client-key.pem"))),
        ] {
            let options = ConnectionOptions {
                connect_timeout: Duration::from_millis(500),
                protocol: RpcProtocol::Binary,
                tls: Some(TlsOptions {
                    accept_invalid_certs: true,
                    client_cert_path: cert.clone(),
                    client_key_path: key.clone(),
                    ..Default::default()
                }),
            };
            let err = match Connection::open(endpoint.clone(), &options) {
                Ok(_) => panic!("half a client identity must fail"),
                Err(e) => e,
            };
            assert!(
                matches!(&err, Error::Client(m) if m.contains("mutual TLS")),
                "cert={cert:?} key={key:?}: got {err:?}"
            );
        }
    }

    /// A missing client key file is a clear client error before any connect.
    #[test]
    fn tls_missing_client_key_file_is_client_error() {
        let endpoint = tls_acceptor_once();
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            tls: Some(TlsOptions {
                accept_invalid_certs: true,
                client_cert_path: Some(fixture("client-cert.pem")),
                client_key_path: Some(fixture("does-not-exist-key.pem")),
                ..Default::default()
            }),
        };
        let err = match Connection::open(endpoint, &options) {
            Ok(_) => panic!("missing client key must fail"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, Error::Client(m) if m.contains("cannot read client key")),
            "got {err:?}"
        );
    }

    /// A file that is not a PKCS#8 key (here: the certificate itself) fails
    /// identity construction with a TLS error, not a panic.
    #[test]
    fn tls_corrupt_client_key_is_tls_error() {
        let endpoint = tls_acceptor_once();
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            tls: Some(TlsOptions {
                accept_invalid_certs: true,
                client_cert_path: Some(fixture("client-cert.pem")),
                client_key_path: Some(fixture("client-cert.pem")), // not a key
                ..Default::default()
            }),
        };
        let err = match Connection::open(endpoint, &options) {
            Ok(_) => panic!("a certificate is not a private key"),
            Err(e) => e,
        };
        assert!(matches!(err, Error::Tls(_)), "got {err:?}");
    }

    /// A missing CA file is a clear client error before any connect.
    #[test]
    fn tls_missing_ca_file_is_client_error() {
        let endpoint = tls_acceptor_once();
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            tls: Some(TlsOptions {
                ca_cert_path: Some(fixture("does-not-exist.pem")),
                ..Default::default()
            }),
        };
        let err = match Connection::open(endpoint, &options) {
            Ok(_) => panic!("missing CA file must fail"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, Error::Client(m) if m.contains("cannot read CA certificate")),
            "got {err:?}"
        );
    }
}
