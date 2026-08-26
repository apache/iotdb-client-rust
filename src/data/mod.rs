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

//! Data structures: TSDataType codes, Value, Tablet, TsBlock, bitmap helpers.
//! Data-type codes must match the official TSFile spec (0–12), identical
//! across all IoTDB client SDKs.

pub mod bitmap;
pub mod record;
pub mod tablet;
pub mod tsblock;
pub mod value;

pub use tablet::Tablet;
pub use tsblock::TsBlock;
pub use value::{object_bytes_to_string, Value};

/// Official TSFile data type codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum TSDataType {
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Float = 3,
    Double = 4,
    Text = 5,
    Vector = 6,
    Unknown = 7,
    Timestamp = 8,
    Date = 9,
    Blob = 10,
    String = 11,
    /// Table-model OBJECT (IoTDB 2.0.8+). Cells carry the already-framed
    /// segment bytes: `[1 byte isEOF][8 byte big-endian offset][content]`
    /// (see `Tablet::build_object_value` / `Tablet::set_object_value_at`).
    Object = 12,
}

impl TSDataType {
    /// The wire code as sent in insert `types` lists (i32) and TsBlock
    /// headers (1 byte).
    pub fn code(self) -> i32 {
        self as i32
    }

    /// Parses a 1-byte type code as found in TsBlock headers.
    pub fn from_code(code: u8) -> Option<TSDataType> {
        Some(match code {
            0 => TSDataType::Boolean,
            1 => TSDataType::Int32,
            2 => TSDataType::Int64,
            3 => TSDataType::Float,
            4 => TSDataType::Double,
            5 => TSDataType::Text,
            6 => TSDataType::Vector,
            7 => TSDataType::Unknown,
            8 => TSDataType::Timestamp,
            9 => TSDataType::Date,
            10 => TSDataType::Blob,
            11 => TSDataType::String,
            12 => TSDataType::Object,
            _ => return None,
        })
    }
}

/// Table-model column category, sent as a signed byte in
/// `TSInsertTabletReq.columnCategories`.
///
/// The server also knows an internal `TIME = 3` category, but it is never
/// sent on the wire — the tablet's timestamps travel in the separate
/// `timestamps` buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum ColumnCategory {
    Tag = 0,
    Field = 1,
    Attribute = 2,
}

impl ColumnCategory {
    /// The wire code as sent in `columnCategories` (signed byte).
    pub fn code(self) -> i8 {
        self as i8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type_codes_match_spec() {
        assert_eq!(TSDataType::Boolean.code(), 0);
        assert_eq!(TSDataType::Int32.code(), 1);
        assert_eq!(TSDataType::Int64.code(), 2);
        assert_eq!(TSDataType::Float.code(), 3);
        assert_eq!(TSDataType::Double.code(), 4);
        assert_eq!(TSDataType::Text.code(), 5);
        assert_eq!(TSDataType::Vector.code(), 6);
        assert_eq!(TSDataType::Unknown.code(), 7);
        assert_eq!(TSDataType::Timestamp.code(), 8);
        assert_eq!(TSDataType::Date.code(), 9);
        assert_eq!(TSDataType::Blob.code(), 10);
        assert_eq!(TSDataType::String.code(), 11);
        assert_eq!(TSDataType::Object.code(), 12);
    }

    #[test]
    fn data_type_from_code_round_trips() {
        for code in 0u8..=12 {
            let ty = TSDataType::from_code(code).expect("valid code");
            assert_eq!(ty.code(), i32::from(code));
        }
        assert_eq!(TSDataType::from_code(13), None);
        assert_eq!(TSDataType::from_code(255), None);
    }

    #[test]
    fn column_category_codes_match_spec() {
        assert_eq!(ColumnCategory::Tag.code(), 0);
        assert_eq!(ColumnCategory::Field.code(), 1);
        assert_eq!(ColumnCategory::Attribute.code(), 2);
    }
}
