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

//! TsBlock decoder — the binary query-result format returned in
//! `queryResult` by the V2 RPCs (protocol spec §5). All integers big-endian.

use super::bitmap::unpack_bits_msb_first;
use super::value::Value;
use super::TSDataType;
use crate::error::{Error, Result};

/// TsBlock column encodings (spec §5.2). `pub(crate)` so the dataset tests
/// can craft synthetic blocks with the same constants.
pub(crate) const ENCODING_BYTE_ARRAY: u8 = 0;
pub(crate) const ENCODING_INT32_ARRAY: u8 = 1;
pub(crate) const ENCODING_INT64_ARRAY: u8 = 2;
pub(crate) const ENCODING_BINARY_ARRAY: u8 = 3;
pub(crate) const ENCODING_RLE: u8 = 4;

/// One decoded TsBlock: `position_count` rows of a time column plus
/// `columns.len()` value columns. Null cells are [`Value::Null`].
#[derive(Debug, Clone, PartialEq)]
pub struct TsBlock {
    pub position_count: usize,
    pub column_types: Vec<TSDataType>,
    /// Time column, always present on the wire — the caller drops it when
    /// `ignoreTimeStamp` is set (spec gotcha #10).
    pub timestamps: Vec<i64>,
    /// `columns[col][row]`; nulls decoded as [`Value::Null`].
    pub columns: Vec<Vec<Value>>,
}

impl TsBlock {
    /// Decodes one serialized TsBlock (one element of `queryResult`).
    ///
    /// Layout (spec §5.1): `valueColumnCount` i32, per-column type bytes,
    /// `positionCount` i32, time-column encoding byte, per-column encoding
    /// bytes, then the time column followed by each value column.
    pub fn decode(bytes: &[u8]) -> Result<TsBlock> {
        let mut r = Reader::new(bytes);

        let value_column_count = r.read_i32()?;
        if value_column_count < 0 {
            return Err(Error::Decode(format!(
                "negative valueColumnCount: {value_column_count}"
            )));
        }
        let value_column_count = value_column_count as usize;

        let mut column_types = Vec::with_capacity(value_column_count);
        for _ in 0..value_column_count {
            let code = r.read_u8()?;
            column_types.push(
                TSDataType::from_code(code)
                    .ok_or_else(|| Error::Decode(format!("unknown TSDataType code {code}")))?,
            );
        }

        let position_count = r.read_i32()?;
        if position_count < 0 {
            return Err(Error::Decode(format!(
                "negative positionCount: {position_count}"
            )));
        }
        let position_count = position_count as usize;

        let time_encoding = r.read_u8()?;
        let mut value_encodings = Vec::with_capacity(value_column_count);
        for _ in 0..value_column_count {
            value_encodings.push(r.read_u8()?);
        }

        // Time column is always present (gotcha #10); decoded as INT64.
        let time_column = decode_column(&mut r, time_encoding, TSDataType::Int64, position_count)?;
        let mut timestamps = Vec::with_capacity(position_count);
        for v in time_column {
            match v {
                Value::Int64(t) => timestamps.push(t),
                other => {
                    return Err(Error::Decode(format!(
                        "time column contains non-INT64 value: {other:?}"
                    )))
                }
            }
        }

        let mut columns = Vec::with_capacity(value_column_count);
        for (i, &ty) in column_types.iter().enumerate() {
            columns.push(decode_column(
                &mut r,
                value_encodings[i],
                ty,
                position_count,
            )?);
        }

        Ok(TsBlock {
            position_count,
            column_types,
            timestamps,
            columns,
        })
    }
}

/// Decodes one column body: null-indicator section, then encoding-specific
/// values (spec §5.3).
fn decode_column(
    r: &mut Reader<'_>,
    encoding: u8,
    ty: TSDataType,
    position_count: usize,
) -> Result<Vec<Value>> {
    match encoding {
        ENCODING_RLE => {
            // Inner-encoding byte, decode ONE value, replicate.
            let inner = r.read_u8()?;
            if inner == ENCODING_RLE {
                return Err(Error::Decode("nested RLE column encoding".into()));
            }
            let single = decode_column(r, inner, ty, 1)?;
            let v = single
                .into_iter()
                .next()
                .ok_or_else(|| Error::Decode("empty RLE inner column".into()))?;
            Ok(vec![v; position_count])
        }
        ENCODING_BYTE_ARRAY => {
            let nulls = read_null_indicators(r, position_count)?;
            // BOOLEAN values: full positionCount bit array (MSB-first),
            // NOT null-skipped — unlike the numeric encodings.
            let bits = read_packed_bits(r, position_count)?;
            Ok((0..position_count)
                .map(|i| {
                    if nulls[i] {
                        Value::Null
                    } else {
                        Value::Boolean(bits[i])
                    }
                })
                .collect())
        }
        ENCODING_INT32_ARRAY => {
            let nulls = read_null_indicators(r, position_count)?;
            let mut out = Vec::with_capacity(position_count);
            for &is_null in &nulls {
                if is_null {
                    out.push(Value::Null);
                    continue; // dense: null positions consume no bytes
                }
                let raw = r.read_i32()?;
                out.push(match ty {
                    TSDataType::Int32 => Value::Int32(raw),
                    TSDataType::Date => Value::Date(raw),
                    TSDataType::Float => Value::Float(f32::from_bits(raw as u32)),
                    _ => {
                        return Err(Error::Decode(format!(
                            "Int32Array encoding with incompatible type {ty:?}"
                        )))
                    }
                });
            }
            Ok(out)
        }
        ENCODING_INT64_ARRAY => {
            let nulls = read_null_indicators(r, position_count)?;
            let mut out = Vec::with_capacity(position_count);
            for &is_null in &nulls {
                if is_null {
                    out.push(Value::Null);
                    continue;
                }
                let raw = r.read_i64()?;
                out.push(match ty {
                    TSDataType::Int64 => Value::Int64(raw),
                    TSDataType::Timestamp => Value::Timestamp(raw),
                    TSDataType::Double => Value::Double(f64::from_bits(raw as u64)),
                    _ => {
                        return Err(Error::Decode(format!(
                            "Int64Array encoding with incompatible type {ty:?}"
                        )))
                    }
                });
            }
            Ok(out)
        }
        ENCODING_BINARY_ARRAY => {
            let nulls = read_null_indicators(r, position_count)?;
            let mut out = Vec::with_capacity(position_count);
            for &is_null in &nulls {
                if is_null {
                    out.push(Value::Null);
                    continue;
                }
                let len = r.read_i32()?;
                if len < 0 {
                    return Err(Error::Decode(format!("negative binary length: {len}")));
                }
                let bytes = r.read_bytes(len as usize)?.to_vec();
                out.push(match ty {
                    TSDataType::Blob => Value::Blob(bytes),
                    TSDataType::Text => Value::Text(decode_utf8(bytes)?),
                    TSDataType::String => Value::String(decode_utf8(bytes)?),
                    _ => {
                        return Err(Error::Decode(format!(
                            "BinaryArray encoding with incompatible type {ty:?}"
                        )))
                    }
                });
            }
            Ok(out)
        }
        other => Err(Error::Decode(format!("unknown column encoding {other}"))),
    }
}

/// Null-indicator section: 1 byte `mayHaveNull`; if non-zero, a
/// `ceil(positionCount/8)`-byte MSB-first bitmap with bit=1 ⇒ null.
fn read_null_indicators(r: &mut Reader<'_>, position_count: usize) -> Result<Vec<bool>> {
    let may_have_null = r.read_u8()?;
    if may_have_null == 0 {
        return Ok(vec![false; position_count]);
    }
    read_packed_bits(r, position_count)
}

/// Reads `count` MSB-first packed bits (`ceil(count/8)` bytes).
fn read_packed_bits(r: &mut Reader<'_>, count: usize) -> Result<Vec<bool>> {
    let bytes = r.read_bytes(count.div_ceil(8))?;
    unpack_bits_msb_first(bytes, count)
        .ok_or_else(|| Error::Decode("truncated packed bit array".into()))
}

fn decode_utf8(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes)
        .map_err(|e| Error::Decode(format!("invalid UTF-8 in text column: {e}")))
}

/// Bounds-checked big-endian cursor over a byte slice.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Reader<'a> {
        Reader { buf, pos: 0 }
    }

    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&e| e <= self.buf.len())
            .ok_or_else(|| {
                Error::Decode(format!(
                    "truncated TsBlock: need {n} bytes at offset {}, have {}",
                    self.pos,
                    self.buf.len() - self.pos
                ))
            })?;
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_bytes(1)?[0])
    }

    fn read_i32(&mut self) -> Result<i32> {
        let b = self.read_bytes(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let b = self.read_bytes(8)?;
        Ok(i64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

/// Encoding helpers for crafting synthetic TsBlocks in unit tests (shared
/// with the dataset state-machine tests).
#[cfg(test)]
pub(crate) mod test_util {
    use super::*;

    /// Builds a TsBlock header: valueColumnCount, type bytes, positionCount,
    /// time encoding (Int64Array), value encodings.
    pub(crate) fn header(types: &[TSDataType], position_count: i32, encodings: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&(types.len() as i32).to_be_bytes());
        for &t in types {
            b.push(t.code() as u8);
        }
        b.extend_from_slice(&position_count.to_be_bytes());
        b.push(ENCODING_INT64_ARRAY); // time column
        b.extend_from_slice(encodings);
        b
    }

    /// Dense Int64Array time column with no nulls.
    pub(crate) fn time_column(ts: &[i64]) -> Vec<u8> {
        let mut b = vec![0u8]; // mayHaveNull = 0
        for t in ts {
            b.extend_from_slice(&t.to_be_bytes());
        }
        b
    }

    /// One-Int32-column block: timestamps `ts`, dense non-null values `vals`.
    pub(crate) fn int32_block(ts: &[i64], vals: &[i32]) -> Vec<u8> {
        assert_eq!(ts.len(), vals.len());
        let mut b = header(
            &[TSDataType::Int32],
            ts.len() as i32,
            &[ENCODING_INT32_ARRAY],
        );
        b.extend_from_slice(&time_column(ts));
        b.push(0); // mayHaveNull
        for v in vals {
            b.extend_from_slice(&v.to_be_bytes());
        }
        b
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::{header, time_column};
    use super::*;

    #[test]
    fn int32_column_no_nulls() {
        let mut b = header(&[TSDataType::Int32], 2, &[ENCODING_INT32_ARRAY]);
        b.extend_from_slice(&time_column(&[10, 20]));
        b.push(0); // mayHaveNull
        b.extend_from_slice(&5i32.to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes());

        let block = TsBlock::decode(&b).unwrap();
        assert_eq!(block.position_count, 2);
        assert_eq!(block.timestamps, vec![10, 20]);
        assert_eq!(block.column_types, vec![TSDataType::Int32]);
        assert_eq!(block.columns, vec![vec![Value::Int32(5), Value::Int32(-1)]]);
    }

    #[test]
    fn nulls_are_dense_skipped_in_numeric_columns() {
        // 3 rows, row 1 null: only 2 i32 values on the wire.
        let mut b = header(&[TSDataType::Int32], 3, &[ENCODING_INT32_ARRAY]);
        b.extend_from_slice(&time_column(&[1, 2, 3]));
        b.push(1); // mayHaveNull
        b.push(0b0100_0000); // MSB-first: position 1 null
        b.extend_from_slice(&7i32.to_be_bytes());
        b.extend_from_slice(&9i32.to_be_bytes());

        let block = TsBlock::decode(&b).unwrap();
        assert_eq!(
            block.columns[0],
            vec![Value::Int32(7), Value::Null, Value::Int32(9)]
        );
    }

    #[test]
    fn int64_double_and_float_bit_patterns() {
        let types = [TSDataType::Double, TSDataType::Float, TSDataType::Timestamp];
        let mut b = header(
            &types,
            1,
            &[
                ENCODING_INT64_ARRAY,
                ENCODING_INT32_ARRAY,
                ENCODING_INT64_ARRAY,
            ],
        );
        b.extend_from_slice(&time_column(&[42]));
        b.push(0);
        b.extend_from_slice(&(-2.5f64).to_be_bytes()); // f64 via i64 bits
        b.push(0);
        b.extend_from_slice(&1.5f32.to_be_bytes());
        b.push(0);
        b.extend_from_slice(&123456789012345i64.to_be_bytes());

        let block = TsBlock::decode(&b).unwrap();
        assert_eq!(block.columns[0], vec![Value::Double(-2.5)]);
        assert_eq!(block.columns[1], vec![Value::Float(1.5)]);
        assert_eq!(block.columns[2], vec![Value::Timestamp(123456789012345)]);
    }

    #[test]
    fn boolean_bytearray_full_bit_array_with_nulls() {
        // 10 rows: nulls at 0 and 9; values MSB-first over ALL positions.
        let mut b = header(&[TSDataType::Boolean], 10, &[ENCODING_BYTE_ARRAY]);
        b.extend_from_slice(&time_column(&(0..10).collect::<Vec<i64>>()));
        b.push(1); // mayHaveNull
        b.extend_from_slice(&[0b1000_0000, 0b0100_0000]); // nulls at 0, 9
                                                          // Value bits (positions 1..=8 meaningful): true at 1, 3, 8.
        b.extend_from_slice(&[0b0101_0000, 0b1000_0000]);

        let block = TsBlock::decode(&b).unwrap();
        let expect: Vec<Value> = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(false),
            Value::Boolean(false),
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Null,
        ];
        assert_eq!(block.columns[0], expect);
    }

    #[test]
    fn binary_array_text_blob_and_nulls() {
        let types = [TSDataType::Text, TSDataType::Blob];
        let mut b = header(&types, 2, &[ENCODING_BINARY_ARRAY, ENCODING_BINARY_ARRAY]);
        b.extend_from_slice(&time_column(&[1, 2]));
        // Text column: ["héllo", null] — null consumes no bytes.
        b.push(1);
        b.push(0b0100_0000);
        let s = "héllo".as_bytes();
        b.extend_from_slice(&(s.len() as i32).to_be_bytes());
        b.extend_from_slice(s);
        // Blob column, no nulls.
        b.push(0);
        b.extend_from_slice(&2i32.to_be_bytes());
        b.extend_from_slice(&[0xCA, 0xFE]);
        b.extend_from_slice(&0i32.to_be_bytes()); // empty blob

        let block = TsBlock::decode(&b).unwrap();
        assert_eq!(
            block.columns[0],
            vec![Value::Text("héllo".into()), Value::Null]
        );
        assert_eq!(
            block.columns[1],
            vec![Value::Blob(vec![0xCA, 0xFE]), Value::Blob(vec![])]
        );
    }

    #[test]
    fn rle_replicates_single_value() {
        let mut b = header(&[TSDataType::Int64], 4, &[ENCODING_RLE]);
        b.extend_from_slice(&time_column(&[1, 2, 3, 4]));
        b.push(ENCODING_INT64_ARRAY); // inner encoding
        b.push(0); // inner mayHaveNull
        b.extend_from_slice(&99i64.to_be_bytes());

        let block = TsBlock::decode(&b).unwrap();
        assert_eq!(block.columns[0], vec![Value::Int64(99); 4]);
    }

    #[test]
    fn rle_of_null_replicates_null() {
        let mut b = header(&[TSDataType::Int32], 3, &[ENCODING_RLE]);
        b.extend_from_slice(&time_column(&[1, 2, 3]));
        b.push(ENCODING_INT32_ARRAY);
        b.push(1); // inner mayHaveNull
        b.push(0b1000_0000); // single position, null → zero dense values
        let block = TsBlock::decode(&b).unwrap();
        assert_eq!(block.columns[0], vec![Value::Null; 3]);
    }

    #[test]
    fn rle_time_column() {
        // Constant time column via RLE (encoding byte in header slot).
        let mut b = Vec::new();
        b.extend_from_slice(&0i32.to_be_bytes()); // no value columns
        b.extend_from_slice(&3i32.to_be_bytes()); // positionCount
        b.push(ENCODING_RLE); // time encoding
        b.push(ENCODING_INT64_ARRAY); // inner
        b.push(0);
        b.extend_from_slice(&7i64.to_be_bytes());

        let block = TsBlock::decode(&b).unwrap();
        assert_eq!(block.timestamps, vec![7, 7, 7]);
        assert!(block.columns.is_empty());
    }

    #[test]
    fn date_decodes_as_yyyymmdd_i32() {
        let mut b = header(&[TSDataType::Date], 1, &[ENCODING_INT32_ARRAY]);
        b.extend_from_slice(&time_column(&[0]));
        b.push(0);
        b.extend_from_slice(&20260710i32.to_be_bytes());
        let block = TsBlock::decode(&b).unwrap();
        assert_eq!(block.columns[0], vec![Value::Date(20260710)]);
    }

    #[test]
    fn empty_block() {
        let mut b = header(&[TSDataType::Int32], 0, &[ENCODING_INT32_ARRAY]);
        b.extend_from_slice(&time_column(&[]));
        b.push(0); // value column: mayHaveNull, zero values
        let block = TsBlock::decode(&b).unwrap();
        assert_eq!(block.position_count, 0);
        assert!(block.timestamps.is_empty());
        assert_eq!(block.columns, vec![Vec::<Value>::new()]);
    }

    #[test]
    fn truncated_and_malformed_inputs_error() {
        // Truncated header.
        assert!(matches!(TsBlock::decode(&[0, 0]), Err(Error::Decode(_))));
        // Unknown type code.
        let mut b = Vec::new();
        b.extend_from_slice(&1i32.to_be_bytes());
        b.push(200);
        assert!(matches!(TsBlock::decode(&b), Err(Error::Decode(_))));
        // Unknown encoding.
        let mut b = header(&[TSDataType::Int32], 1, &[9]);
        b.extend_from_slice(&time_column(&[1]));
        assert!(matches!(TsBlock::decode(&b), Err(Error::Decode(_))));
        // Truncated value payload.
        let mut b = header(&[TSDataType::Int64], 1, &[ENCODING_INT64_ARRAY]);
        b.extend_from_slice(&time_column(&[1]));
        b.push(0);
        b.extend_from_slice(&[0, 0]); // only 2 of 8 bytes
        assert!(matches!(TsBlock::decode(&b), Err(Error::Decode(_))));
        // Nested RLE is rejected.
        let mut b = header(&[TSDataType::Int32], 1, &[ENCODING_RLE]);
        b.extend_from_slice(&time_column(&[1]));
        b.push(ENCODING_RLE);
        assert!(matches!(TsBlock::decode(&b), Err(Error::Decode(_))));
    }
}
