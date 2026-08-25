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
#[cfg(feature = "tls")]
use std::sync::Arc;
use std::time::{Duration, Instant};

use socket2::SockRef;

#[cfg(feature = "tls")]
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
#[cfg(feature = "tls")]
use rustls::crypto::WebPkiSupportedAlgorithms;
#[cfg(feature = "tls")]
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
#[cfg(feature = "tls")]
use rustls::{
    ClientConfig, ClientConnection, DigitallySignedStruct, RootCertStore, SignatureScheme,
    StreamOwned,
};

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
    /// server's self-signed certificate), in addition to the platform roots.
    pub ca_cert_path: Option<std::path::PathBuf>,
    /// Skip certificate-chain and hostname verification (self-signed test
    /// certs). TLS handshake signatures are still verified.
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

/// How to open a [`Connection`]: timeouts, wire protocol, optional TLS.
#[derive(Debug, Clone)]
pub struct ConnectionOptions {
    /// Total TCP connect timeout per endpoint attempt (shared across every
    /// resolved address of that endpoint). Default 10 s.
    pub connect_timeout: Duration,
    /// Client-side bound on each blocking socket read/write (SO_RCVTIMEO /
    /// SO_SNDTIMEO), applied after the TCP connect and **before** the TLS
    /// handshake, so it bounds the handshake, every RPC read and the
    /// best-effort drop-time `closeSession` alike. `None` restores the
    /// old unbounded blocking behaviour. Default 60 s (matches the default
    /// server-side `query_timeout_ms`).
    pub socket_timeout: Option<Duration>,
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
            socket_timeout: Some(Duration::from_secs(60)),
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

    /// Loose equality for redirect-hint matching: the port must match and
    /// the host compares case-insensitively after trimming and stripping
    /// IPv6 brackets; loopback spellings (`localhost`, `127.x.y.z`,
    /// `::1`) are equivalent to each other. General hostname-vs-IP
    /// resolution would need DNS and is deliberately not done here.
    pub fn equivalent(&self, other: &Self) -> bool {
        self.port == other.port
            && (normalized_host(&self.host) == normalized_host(&other.host)
                || (is_loopback_host(&self.host) && is_loopback_host(&other.host)))
    }
}

fn normalized_host(host: &str) -> String {
    host.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn is_loopback_host(host: &str) -> bool {
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
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
    /// `options.connect_timeout`; every subsequent socket read/write is
    /// bounded by `options.socket_timeout`), optionally wrap it in TLS, and
    /// stack framed transport + the selected protocol on top.
    pub fn open(endpoint: Endpoint, options: &ConnectionOptions) -> Result<Self> {
        let stream = connect_stream(&endpoint, options.connect_timeout, options.socket_timeout)?;

        #[cfg(feature = "tls")]
        if let Some(tls) = &options.tls {
            let stream = tls_handshake(&endpoint, stream, tls, options.socket_timeout)?;
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

/// Resolve the endpoint and try each resolved address, sharing one total
/// `connect_timeout` budget across all of them (a multi-address hostname
/// must not multiply the configured bound), then apply `socket_timeout`
/// and TCP keepalive before handing the stream up.
fn connect_stream(
    endpoint: &Endpoint,
    connect_timeout: Duration,
    socket_timeout: Option<Duration>,
) -> Result<TcpStream> {
    let addrs = (endpoint.host.as_str(), endpoint.port)
        .to_socket_addrs()
        .map_err(thrift::Error::from)?;
    // A single deadline for the whole endpoint attempt. `checked_add`
    // treats Duration::MAX as "no bound" instead of panicking on overflow.
    let deadline = Instant::now().checked_add(connect_timeout);
    let mut last_err: Option<std::io::Error> = None;
    for addr in addrs {
        let attempt_timeout = match deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    last_err = Some(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!("connect to {endpoint} timed out"),
                    ));
                    break;
                }
                remaining
            }
            None => connect_timeout,
        };
        match connect_one(addr, attempt_timeout, socket_timeout) {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }
    Err(match last_err {
        Some(e) => Error::Thrift(thrift::Error::from(e)),
        None => Error::Client(format!("could not resolve endpoint {endpoint}")),
    })
}

/// Connect one resolved address with the remaining endpoint budget and
/// configure the socket: TCP_NODELAY, SO_KEEPALIVE, and the optional
/// SO_RCVTIMEO/SO_SNDTIMEO that bounds every later read/write on this
/// connection (TLS handshake included).
fn connect_one(
    addr: std::net::SocketAddr,
    connect_timeout: Duration,
    socket_timeout: Option<Duration>,
) -> std::io::Result<TcpStream> {
    // std's connect_timeout keeps the established cross-platform connect
    // semantics; SockRef then applies the options on the existing socket.
    let stream = TcpStream::connect_timeout(&addr, connect_timeout)?;
    let socket = SockRef::from(&stream);
    socket.set_nodelay(true)?;
    socket.set_keepalive(true)?;
    // A zero duration would select non-blocking mode on some platforms;
    // treat it as "no timeout" instead (None is the documented way).
    if let Some(timeout) = socket_timeout.filter(|timeout| !timeout.is_zero()) {
        socket.set_read_timeout(Some(timeout))?;
        socket.set_write_timeout(Some(timeout))?;
    }
    Ok(stream)
}

/// Run the TLS handshake over an established TCP stream. The stream
/// already carries the socket-level read/write timeout, so a peer that
/// accepts and then stalls the handshake fails here instead of blocking
/// forever.
#[cfg(feature = "tls")]
fn tls_handshake(
    endpoint: &Endpoint,
    mut stream: TcpStream,
    tls: &TlsOptions,
    socket_timeout: Option<Duration>,
) -> Result<StreamOwned<ClientConnection, TcpStream>> {
    let config = tls_client_config(tls)?;
    let domain = tls.domain_override.as_deref().unwrap_or(&endpoint.host);
    let server_name = ServerName::try_from(domain.to_owned())
        .map_err(|e| Error::Client(format!("invalid TLS server name '{domain}': {e}")))?;
    let mut connection =
        ClientConnection::new(config, server_name).map_err(|e| Error::Tls(e.to_string()))?;

    connection.complete_io(&mut stream).map_err(|e| {
        // A blocking socket with SO_RCVTIMEO reports WouldBlock/TimedOut
        // when the peer stalls the handshake: turn it into an actionable
        // error instead of leaking the OS error kind.
        if matches!(
            e.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ) {
            let bound = socket_timeout.map_or("no socket timeout".into(), |t| format!("{t:?}"));
            return Error::Tls(format!("TLS handshake timed out ({bound})"));
        }
        Error::Tls(e.to_string())
    })?;
    if connection.is_handshaking() {
        return Err(Error::Tls("TLS handshake did not complete".into()));
    }

    Ok(StreamOwned::new(connection, stream))
}

#[cfg(feature = "tls")]
fn tls_client_config(tls: &TlsOptions) -> Result<Arc<ClientConfig>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let extra_roots = match &tls.ca_cert_path {
        Some(path) => load_certificates(path, "CA certificate")?,
        None => Vec::new(),
    };

    let verifier: Arc<dyn ServerCertVerifier> = if tls.accept_invalid_certs {
        Arc::new(NoCertificateVerification::new(
            provider.signature_verification_algorithms,
        ))
    } else {
        let native_roots = rustls_native_certs::load_native_certs();
        for error in native_roots.errors {
            log::warn!("cannot load a native CA certificate: {error}");
        }

        let mut roots = RootCertStore::empty();
        let (_, ignored) = roots.add_parsable_certificates(native_roots.certs);
        if ignored != 0 {
            log::warn!("ignored {ignored} native CA certificate(s) that WebPKI cannot parse");
        }
        for certificate in extra_roots {
            roots
                .add(certificate)
                .map_err(|e| Error::Tls(format!("cannot add CA certificate: {e}")))?;
        }

        rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|e| Error::Tls(e.to_string()))?
    };

    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Tls(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(verifier);

    let config = match (&tls.client_cert_path, &tls.client_key_path) {
        (Some(cert_path), Some(key_path)) => {
            let certificates = load_certificates(cert_path, "client certificate")?;
            let key_file = std::fs::File::open(key_path).map_err(|e| {
                Error::Client(format!(
                    "cannot read client key {}: {e}",
                    key_path.display()
                ))
            })?;
            let mut key_reader = std::io::BufReader::new(key_file);
            let key = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
                .next()
                .transpose()
                .map_err(|e| Error::Tls(format!("cannot parse client key: {e}")))?
                .ok_or_else(|| Error::Tls("client key is not a PEM PKCS#8 private key".into()))?;
            builder
                .with_client_auth_cert(certificates, key.into())
                .map_err(|e| Error::Tls(e.to_string()))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(Error::Client(
                "mutual TLS requires both client_cert_path and client_key_path".into(),
            ))
        }
    };

    Ok(Arc::new(config))
}

#[cfg(feature = "tls")]
fn load_certificates(
    path: &std::path::Path,
    description: &str,
) -> Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)
        .map_err(|e| Error::Client(format!("cannot read {description} {}: {e}", path.display())))?;
    let mut reader = std::io::BufReader::new(file);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| Error::Tls(format!("cannot parse {description}: {e}")))?;
    if certificates.is_empty() {
        return Err(Error::Tls(format!(
            "{description} {} contains no PEM certificates",
            path.display()
        )));
    }
    Ok(certificates)
}

/// Disables certificate-chain and hostname validation while retaining
/// cryptographic verification of the TLS handshake signatures.
#[cfg(feature = "tls")]
#[derive(Debug)]
struct NoCertificateVerification {
    supported_algorithms: WebPkiSupportedAlgorithms,
}

#[cfg(feature = "tls")]
impl NoCertificateVerification {
    fn new(supported_algorithms: WebPkiSupportedAlgorithms) -> Self {
        Self {
            supported_algorithms,
        }
    }
}

#[cfg(feature = "tls")]
impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.supported_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.supported_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported_algorithms.supported_schemes()
    }
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
struct SharedTlsStream(std::sync::Arc<std::sync::Mutex<StreamOwned<ClientConnection, TcpStream>>>);

#[cfg(feature = "tls")]
impl SharedTlsStream {
    fn new(stream: StreamOwned<ClientConnection, TcpStream>) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(stream)))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StreamOwned<ClientConnection, TcpStream>> {
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

    /// F11: redirect-hint matching must tolerate case/bracket/loopback
    /// spelling differences, but still require the same port.
    #[test]
    fn endpoint_equivalent_normalizes_and_matches_loopback() {
        assert!(Endpoint::new("LOCALHOST", 6667).equivalent(&Endpoint::new("localhost", 6667)));
        assert!(Endpoint::new("localhost", 6667).equivalent(&Endpoint::new("127.0.0.1", 6667)));
        assert!(Endpoint::new("127.0.0.1", 6667).equivalent(&Endpoint::new("::1", 6667)));
        assert!(Endpoint::new("[::1]", 6667).equivalent(&Endpoint::new("::1", 6667)));
        assert!(!Endpoint::new("localhost", 6667).equivalent(&Endpoint::new("localhost", 6668)));
        assert!(
            !Endpoint::new("localhost", 6667).equivalent(&Endpoint::new("iotdb.example.com", 6667))
        );
    }

    #[test]
    fn default_options_are_binary_no_tls() {
        let options = ConnectionOptions::default();
        assert_eq!(options.connect_timeout, Duration::from_secs(10));
        assert_eq!(options.socket_timeout, Some(Duration::from_secs(60)));
        assert_eq!(options.protocol, RpcProtocol::Binary);
        #[cfg(feature = "tls")]
        assert!(options.tls.is_none());
    }

    /// A local listener that accepts connections and then stays silent —
    /// the equivalent of a peer that finished the TCP handshake and never
    /// replies (GC pause, dropped firewall state, accepting LB). The accept
    /// thread parks while holding the stream so it never sends EOF.
    pub(super) fn silent_listener() -> Endpoint {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("local_addr").port();
        std::thread::spawn(move || {
            let (_stream, _) = listener.accept().expect("accept");
            std::thread::park();
        });
        Endpoint::new("127.0.0.1", port)
    }

    /// `socket_timeout` must bound reads after the TCP handshake: against
    /// a peer that accepts and never replies, the RPC returns a Thrift
    /// error around the configured bound instead of blocking forever.
    #[test]
    fn socket_timeout_bounds_reads_after_handshake() {
        use crate::protocol::client::TIClientRPCServiceSyncClient;

        let endpoint = silent_listener();
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            socket_timeout: Some(Duration::from_millis(300)),
            ..Default::default()
        };
        let mut connection = Connection::open(endpoint, &options).expect("TCP connect succeeds");
        let started = Instant::now();
        let err = connection
            .client_mut()
            .request_statement_id(1)
            .expect_err("silent peer must not block forever");
        assert!(matches!(err, thrift::Error::Transport(_)), "got {err:?}");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "read took {elapsed:?}, socket timeout not applied"
        );
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
    use std::sync::Arc;

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/tls")
            .join(name)
    }

    fn pkcs8_key(name: &str) -> rustls::pki_types::PrivateKeyDer<'static> {
        let file = std::fs::File::open(fixture(name)).expect("read key fixture");
        let mut reader = std::io::BufReader::new(file);
        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut reader);
        keys.next()
            .expect("PKCS#8 key item")
            .expect("parse PKCS#8 key")
            .into()
    }

    fn server_config(require_client_auth: bool) -> Arc<rustls::ServerConfig> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .expect("protocol versions");
        let builder = if require_client_auth {
            let mut roots = rustls::RootCertStore::empty();
            for certificate in
                load_certificates(&fixture("client-cert.pem"), "client root").expect("client root")
            {
                roots.add(certificate).expect("add client root");
            }
            let verifier =
                rustls::server::WebPkiClientVerifier::builder_with_provider(roots.into(), provider)
                    .build()
                    .expect("client verifier");
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        };
        let config = builder
            .with_single_cert(
                load_certificates(&fixture("cert.pem"), "server certificate")
                    .expect("server certificate"),
                pkcs8_key("key.pem"),
            )
            .expect("server config");
        Arc::new(config)
    }

    /// Spawn a rustls acceptor on a loopback port that completes handshakes
    /// and then drops each connection. Uses the checked-in self-signed cert
    /// (CN=localhost, SAN DNS:localhost + IP:127.0.0.1).
    fn tls_acceptor_once() -> Endpoint {
        tls_acceptor(false).0
    }

    fn tls_acceptor(
        require_client_auth: bool,
    ) -> (
        Endpoint,
        std::sync::mpsc::Receiver<std::result::Result<(), String>>,
    ) {
        let config = server_config(require_client_auth);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("local_addr").port();
        let (handshake_result_tx, handshake_result_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = match listener.accept() {
                Ok((mut stream, _)) => match rustls::ServerConnection::new(config) {
                    Ok(mut connection) => connection
                        .complete_io(&mut stream)
                        .map(|_| ())
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error.to_string()),
                },
                Err(error) => Err(error.to_string()),
            };
            // Most tests only need the client-side result and deliberately
            // discard this receiver. The mutual-TLS test asserts it below.
            let _ = handshake_result_tx.send(result);
        });
        (Endpoint::new("127.0.0.1", port), handshake_result_rx)
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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

    /// A socket timeout bounds the TLS handshake itself: against a peer
    /// that accepts and then stays silent, `Connection::open` fails with a
    /// TLS timeout error around the configured bound instead of blocking
    /// forever.
    #[test]
    fn tls_handshake_times_out_against_silent_peer() {
        let endpoint = super::tests::silent_listener();
        let options = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            socket_timeout: Some(Duration::from_millis(300)),
            protocol: RpcProtocol::Binary,
            tls: Some(TlsOptions {
                accept_invalid_certs: true,
                ..Default::default()
            }),
        };
        let started = Instant::now();
        let err = match Connection::open(endpoint, &options) {
            Ok(_) => panic!("silent peer must not complete a TLS handshake"),
            Err(e) => e,
        };
        assert!(matches!(err, Error::Tls(_)), "got {err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "TLS handshake was not bounded by socket_timeout"
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
            ..Default::default()
        };
        Connection::open(endpoint.clone(), &plain).expect("plain open against plain listener");

        let tls = ConnectionOptions {
            connect_timeout: Duration::from_millis(500),
            protocol: RpcProtocol::Binary,
            tls: Some(TlsOptions {
                accept_invalid_certs: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = match Connection::open(endpoint, &tls) {
            Ok(_) => panic!("TLS handshake against a plain endpoint must fail"),
            Err(e) => e,
        };
        assert!(matches!(err, Error::Tls(_)), "got {err:?}");
    }

    /// Mutual TLS: a PEM client certificate + PKCS#8 key load into the
    /// client config, and a rustls server that requires the fixture client
    /// certificate completes the handshake.
    #[test]
    fn tls_client_identity_handshake_succeeds() {
        let (endpoint, server_handshake) = tls_acceptor(true);
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
            ..Default::default()
        };
        let connection = Connection::open(endpoint, &options).expect("TLS handshake with identity");
        assert_eq!(connection.protocol(), RpcProtocol::Binary);
        server_handshake
            .recv_timeout(Duration::from_secs(5))
            .expect("server handshake result")
            .expect("server accepted client identity");
    }

    /// Setting only one of the client cert/key pair is a config error
    /// caught before any I/O.
    #[test]
    fn tls_client_identity_requires_both_paths() {
        for (cert, key) in [
            (Some(fixture("client-cert.pem")), None),
            (None, Some(fixture("client-key.pem"))),
        ] {
            let endpoint = tls_acceptor_once();
            let options = ConnectionOptions {
                connect_timeout: Duration::from_millis(500),
                protocol: RpcProtocol::Binary,
                tls: Some(TlsOptions {
                    accept_invalid_certs: true,
                    client_cert_path: cert.clone(),
                    client_key_path: key.clone(),
                    ..Default::default()
                }),
                ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
