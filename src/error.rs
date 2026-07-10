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

//! Error types for the IoTDB client.

use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// Underlying Thrift transport/protocol failure.
    Thrift(thrift::Error),
    /// Non-success status code returned by the server (TSStatus).
    Server { code: i32, message: String },
    /// Client-side usage or state error (e.g. session not open).
    Client(String),
    /// Malformed binary payload received from the server (e.g. truncated TsBlock).
    Decode(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Thrift(e) => write!(f, "thrift error: {e}"),
            Error::Server { code, message } => write!(f, "server error {code}: {message}"),
            Error::Client(msg) => write!(f, "client error: {msg}"),
            Error::Decode(msg) => write!(f, "decode error: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<thrift::Error> for Error {
    fn from(e: thrift::Error) -> Self {
        Error::Thrift(e)
    }
}
