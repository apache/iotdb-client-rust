//! Low-level Thrift connection to an IoTDB node.
//!
//! Mirrors `src/connection/Connection.ts` (Node.js) and the Thrift client setup
//! in the C# SDK: TCP → TFramedTransport → TBinaryProtocol → IClientRPCService client.

use crate::error::Result;

/// A single endpoint `host:port` of an IoTDB DataNode (default port 6667).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

impl Endpoint {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self { host: host.into(), port }
    }
}

/// Low-level connection wrapper. Owns the Thrift transport/protocol pair
/// and the generated `IClientRPCService` client once codegen lands.
pub struct Connection {
    endpoint: Endpoint,
    // TODO(codegen): hold IClientRPCServiceSyncClient over TFramedTransport + TBinaryProtocol
}

impl Connection {
    pub fn open(endpoint: Endpoint) -> Result<Self> {
        // TODO(codegen): establish TTcpChannel, wrap in framed transport + binary protocol
        Ok(Self { endpoint })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}
