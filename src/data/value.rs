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

//! A dynamically-typed IoTDB cell value.

use super::TSDataType;
use crate::error::{Error, Result};

/// One cell of an IoTDB row: a typed scalar or `Null`.
///
/// `Date` carries an `i32` in `yyyyMMdd` form (e.g. 2026-07-10 →
/// `20260710`), matching the C#/Java wire encoding. `Timestamp` is epoch
/// milliseconds.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    Text(String),
    Timestamp(i64),
    /// Date as i32 `yyyyMMdd` (e.g. `20260710`).
    Date(i32),
    Blob(Vec<u8>),
    String(String),
    /// Table-model OBJECT cell. The bytes are the **already-framed OBJECT
    /// segment**: `[1 byte isEOF][8 byte big-endian offset][content]`
    /// (see `Tablet::build_object_value` / `Tablet::set_object_value_at`).
    Object(Vec<u8>),
    Null,
}

impl Value {
    /// The [`TSDataType`] this value carries, or `None` for [`Value::Null`].
    pub fn data_type(&self) -> Option<TSDataType> {
        Some(match self {
            Value::Boolean(_) => TSDataType::Boolean,
            Value::Int32(_) => TSDataType::Int32,
            Value::Int64(_) => TSDataType::Int64,
            Value::Float(_) => TSDataType::Float,
            Value::Double(_) => TSDataType::Double,
            Value::Text(_) => TSDataType::Text,
            Value::Timestamp(_) => TSDataType::Timestamp,
            Value::Date(_) => TSDataType::Date,
            Value::Blob(_) => TSDataType::Blob,
            Value::String(_) => TSDataType::String,
            Value::Object(_) => TSDataType::Object,
            Value::Null => return None,
        })
    }

    /// The wire type code (§8 of the protocol spec), or `None` for `Null`.
    pub fn type_code(&self) -> Option<i32> {
        self.data_type().map(TSDataType::code)
    }

    /// True iff this is [`Value::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// Formats the wire representation of a stored OBJECT value for display.
///
/// The server stores OBJECT cells as an 8-byte big-endian file size followed
/// by the internal object path. Mirroring the Go client's
/// `objectBytesToString` and the Node/C# helpers, this renders the size as
/// `(Object) 1023 B` / `(Object) 1.00 KB` / `(Object) 1.00 MB` /
/// `(Object) 1.00 GB`.
pub fn object_bytes_to_string(bytes: &[u8]) -> Result<String> {
    if bytes.len() < 8 {
        return Err(Error::Decode(format!(
            "invalid OBJECT value: expected at least 8 bytes, got {}",
            bytes.len()
        )));
    }
    let mut size_bytes = [0u8; 8];
    size_bytes.copy_from_slice(&bytes[..8]);
    let size = u64::from_be_bytes(size_bytes);
    const KILOBYTE: f64 = 1024.0;
    const MEGABYTE: f64 = KILOBYTE * 1024.0;
    const GIGABYTE: f64 = MEGABYTE * 1024.0;
    let size = size as f64;
    if size < KILOBYTE {
        Ok(format!("(Object) {size:.0} B"))
    } else if size < MEGABYTE {
        Ok(format!("(Object) {:.2} KB", size / KILOBYTE))
    } else if size < GIGABYTE {
        Ok(format!("(Object) {:.2} MB", size / MEGABYTE))
    } else {
        Ok(format!("(Object) {:.2} GB", size / GIGABYTE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_codes() {
        assert_eq!(Value::Boolean(true).type_code(), Some(0));
        assert_eq!(Value::Int32(1).type_code(), Some(1));
        assert_eq!(Value::Int64(1).type_code(), Some(2));
        assert_eq!(Value::Float(1.0).type_code(), Some(3));
        assert_eq!(Value::Double(1.0).type_code(), Some(4));
        assert_eq!(Value::Text("t".into()).type_code(), Some(5));
        assert_eq!(Value::Timestamp(0).type_code(), Some(8));
        assert_eq!(Value::Date(20260710).type_code(), Some(9));
        assert_eq!(Value::Blob(vec![0]).type_code(), Some(10));
        assert_eq!(Value::String("s".into()).type_code(), Some(11));
        assert_eq!(Value::Object(vec![]).type_code(), Some(12));
        assert_eq!(Value::Null.type_code(), None);
    }

    #[test]
    fn data_type_of_null_is_none() {
        assert_eq!(Value::Null.data_type(), None);
        assert!(Value::Null.is_null());
        assert!(!Value::Int32(7).is_null());
    }

    fn object_size_bytes(size: u64) -> Vec<u8> {
        let mut value = size.to_be_bytes().to_vec();
        value.extend_from_slice(b"internal/path/1.bin");
        value
    }

    #[test]
    fn object_bytes_to_string_formats_all_units() {
        assert_eq!(
            object_bytes_to_string(&object_size_bytes(1023)).unwrap(),
            "(Object) 1023 B"
        );
        assert_eq!(
            object_bytes_to_string(&object_size_bytes(1024)).unwrap(),
            "(Object) 1.00 KB"
        );
        assert_eq!(
            object_bytes_to_string(&object_size_bytes(1024 * 1024)).unwrap(),
            "(Object) 1.00 MB"
        );
        assert_eq!(
            object_bytes_to_string(&object_size_bytes(1024 * 1024 * 1024)).unwrap(),
            "(Object) 1.00 GB"
        );
    }

    #[test]
    fn object_bytes_to_string_rejects_short_input() {
        assert!(matches!(
            object_bytes_to_string(&[0; 7]),
            Err(Error::Decode(m)) if m.contains("at least 8 bytes")
        ));
    }
}
