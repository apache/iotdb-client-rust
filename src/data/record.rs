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

//! Row-oriented value-buffer serialization for the `insertRecord(s)` RPC
//! family. Unlike the tablet's column-major layout (spec §3.2), a record
//! interleaves type and value per field: each cell is a 1-byte TSDataType
//! marker followed by its big-endian payload.

use super::tablet::write_binary;
use super::value::Value;

/// Java `SessionUtils.TYPE_NULL` (`-2`): a null cell is this bare marker
/// byte with no payload — the server's `InsertRowNode` reads `-2` as
/// null-without-type.
const TYPE_NULL: u8 = 0xFE;

/// Serializes one record row for `insertRecord`/`insertRecords`/
/// `insertRecordsOfOneDevice`: per value a 1-byte type-code marker plus the
/// big-endian payload (TEXT/STRING/BLOB carry a 4-byte BE length prefix;
/// DATE is i32 `yyyyMMdd`; BOOLEAN is one byte). [`Value::Null`] cells are
/// the bare [`TYPE_NULL`] marker, matching the Java client.
pub fn serialize_record_values(values: &[Value]) -> Vec<u8> {
    let mut buf = Vec::new();
    for value in values {
        match value {
            Value::Null => buf.push(TYPE_NULL),
            other => {
                let code = other.type_code().expect("non-null value carries a type");
                buf.push(code as u8);
                match other {
                    Value::Boolean(b) => buf.push(u8::from(*b)),
                    Value::Int32(v) | Value::Date(v) => buf.extend_from_slice(&v.to_be_bytes()),
                    Value::Int64(v) | Value::Timestamp(v) => {
                        buf.extend_from_slice(&v.to_be_bytes())
                    }
                    Value::Float(v) => buf.extend_from_slice(&v.to_be_bytes()),
                    Value::Double(v) => buf.extend_from_slice(&v.to_be_bytes()),
                    Value::Text(s) | Value::String(s) => write_binary(&mut buf, s.as_bytes()),
                    // OBJECT uses the same length-prefixed binary layout;
                    // the server rejects tree-model OBJECTs, but the SDK does
                    // not add an extra client-side restriction (plan §1).
                    Value::Blob(b) | Value::Object(b) => write_binary(&mut buf, b),
                    Value::Null => unreachable!("handled above"),
                }
            }
        }
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_types_known_bytes() {
        let values = [
            Value::Boolean(true),
            Value::Int32(7),
            Value::Int64(-2),
            Value::Float(1.5),
            Value::Double(-0.5),
            Value::Text("ab".into()),
            Value::Timestamp(3),
            Value::Date(20260713),
            Value::Blob(vec![0xDE, 0xAD]),
            Value::String("é".into()),
            Value::Object(vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0xDE, 0xAD]),
        ];

        let mut expected: Vec<u8> = Vec::new();
        expected.extend_from_slice(&[0x00, 0x01]); // BOOLEAN true
        expected.push(0x01);
        expected.extend_from_slice(&7i32.to_be_bytes());
        expected.push(0x02);
        expected.extend_from_slice(&(-2i64).to_be_bytes());
        expected.push(0x03);
        expected.extend_from_slice(&[0x3F, 0xC0, 0x00, 0x00]); // 1.5f32 IEEE 754 BE
        expected.push(0x04);
        expected.extend_from_slice(&(-0.5f64).to_be_bytes());
        expected.extend_from_slice(&[0x05, 0, 0, 0, 2, b'a', b'b']);
        expected.push(0x08);
        expected.extend_from_slice(&3i64.to_be_bytes());
        expected.push(0x09);
        expected.extend_from_slice(&20260713i32.to_be_bytes()); // yyyyMMdd
        expected.extend_from_slice(&[0x0A, 0, 0, 0, 2, 0xDE, 0xAD]);
        expected.extend_from_slice(&[0x0B, 0, 0, 0, 2, 0xC3, 0xA9]); // "é" UTF-8
                                                                     // OBJECT: type marker 12 + length-prefixed framed segment.
        expected.extend_from_slice(&[0x0C, 0, 0, 0, 11, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0xDE, 0xAD]);
        assert_eq!(serialize_record_values(&values), expected);
    }

    #[test]
    fn null_is_bare_type_marker() {
        assert_eq!(serialize_record_values(&[Value::Null]), [0xFE]);
        // Nulls interleave with typed cells without payload bytes.
        let buf = serialize_record_values(&[
            Value::Int32(5),
            Value::Null,
            Value::Boolean(false),
            Value::Null,
        ]);
        assert_eq!(buf, [0x01, 0, 0, 0, 5, 0xFE, 0x00, 0x00, 0xFE]);
    }

    #[test]
    fn empty_row_is_empty_buffer() {
        assert_eq!(serialize_record_values(&[]), Vec::<u8>::new());
    }
}
