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

//! A batch of rows for one device (tree model) or one table (table model),
//! serialized column-major for `insertTablet` (protocol spec §3).

use super::bitmap::pack_bits_lsb_first;
use super::value::Value;
use super::{ColumnCategory, TSDataType};
use crate::error::{Error, Result};

/// A column-major batch of rows for `insertTablet`.
///
/// Tree model: `target` is the device id (e.g. `root.sg.d1`) and
/// `column_categories` is `None`. Table model (via [`Tablet::new_table`]):
/// `target` is the table name and every column carries a
/// [`ColumnCategory`].
///
/// Rows are stably sorted by timestamp before serialization (spec §3.5);
/// the server fast path assumes sorted input.
#[derive(Debug, Clone)]
pub struct Tablet {
    /// Device id (tree model) or table name (table model).
    target: String,
    /// Measurement names (tree model) or column names (table model).
    measurements: Vec<String>,
    types: Vec<TSDataType>,
    timestamps: Vec<i64>,
    /// Column-major: `values[col][row]`, `None` = null cell.
    values: Vec<Vec<Option<Value>>>,
    /// Table model only: one category per column.
    column_categories: Option<Vec<ColumnCategory>>,
    /// Tree model only: write to an aligned device. Always `false` on the
    /// wire for table-model tablets (spec §6).
    aligned: bool,
}

impl Tablet {
    /// Creates an empty tree-model tablet for `device_id`.
    pub fn new(
        device_id: impl Into<String>,
        measurements: Vec<String>,
        types: Vec<TSDataType>,
    ) -> Result<Tablet> {
        if measurements.len() != types.len() {
            return Err(Error::Client(format!(
                "measurement count ({}) != type count ({})",
                measurements.len(),
                types.len()
            )));
        }
        let columns = measurements.len();
        Ok(Tablet {
            target: device_id.into(),
            measurements,
            types,
            timestamps: Vec::new(),
            values: vec![Vec::new(); columns],
            column_categories: None,
            aligned: false,
        })
    }

    /// Creates an empty tree-model tablet for an **aligned** device.
    pub fn new_aligned(
        device_id: impl Into<String>,
        measurements: Vec<String>,
        types: Vec<TSDataType>,
    ) -> Result<Tablet> {
        let mut tablet = Tablet::new(device_id, measurements, types)?;
        tablet.aligned = true;
        Ok(tablet)
    }

    /// Creates an empty table-model tablet for `table_name` with one
    /// [`ColumnCategory`] per column.
    pub fn new_table(
        table_name: impl Into<String>,
        column_names: Vec<String>,
        types: Vec<TSDataType>,
        column_categories: Vec<ColumnCategory>,
    ) -> Result<Tablet> {
        if column_categories.len() != types.len() {
            return Err(Error::Client(format!(
                "category count ({}) != type count ({})",
                column_categories.len(),
                types.len()
            )));
        }
        let mut tablet = Tablet::new(table_name, column_names, types)?;
        tablet.column_categories = Some(column_categories);
        Ok(tablet)
    }

    /// Device id (tree model) or table name (table model).
    pub fn target(&self) -> &str {
        &self.target
    }

    /// Table name — alias of [`Tablet::target`] for table-model tablets.
    pub fn table_name(&self) -> &str {
        &self.target
    }

    pub fn measurements(&self) -> &[String] {
        &self.measurements
    }

    pub fn types(&self) -> &[TSDataType] {
        &self.types
    }

    pub fn timestamps(&self) -> &[i64] {
        &self.timestamps
    }

    /// `Some` iff this is a table-model tablet.
    pub fn column_categories(&self) -> Option<&[ColumnCategory]> {
        self.column_categories.as_deref()
    }

    /// True for aligned-device tree-model tablets (spec §3.1 field 8).
    /// Always `false` for table-model tablets.
    pub fn is_aligned(&self) -> bool {
        self.aligned
    }

    pub fn row_count(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_table_model(&self) -> bool {
        self.column_categories.is_some()
    }

    /// Appends one row. `row[i]` must be `None` (null) or a [`Value`] whose
    /// type matches `types[i]`.
    pub fn add_row(&mut self, timestamp: i64, row: Vec<Option<Value>>) -> Result<()> {
        if row.len() != self.types.len() {
            return Err(Error::Client(format!(
                "row has {} cells, tablet has {} columns",
                row.len(),
                self.types.len()
            )));
        }
        for (i, cell) in row.iter().enumerate() {
            match cell {
                None | Some(Value::Null) => {}
                Some(v) if v.data_type() == Some(self.types[i]) => {}
                Some(v) => {
                    return Err(Error::Client(format!(
                        "column {i} ({}) expects {:?}, got {v:?}",
                        self.measurements[i], self.types[i]
                    )));
                }
            }
        }
        self.timestamps.push(timestamp);
        for (col, cell) in self.values.iter_mut().zip(row) {
            // Normalize Some(Null) to None so null detection is uniform.
            col.push(cell.filter(|v| !v.is_null()));
        }
        Ok(())
    }

    /// Stably sorts rows by timestamp, reordering all value columns in step.
    pub fn sort_by_timestamp(&mut self) {
        let n = self.timestamps.len();
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&i| self.timestamps[i]); // stable
        if order.iter().enumerate().all(|(pos, &i)| pos == i) {
            return;
        }
        self.timestamps = order.iter().map(|&i| self.timestamps[i]).collect();
        for col in &mut self.values {
            *col = order.iter().map(|&i| col[i].clone()).collect();
        }
    }

    /// Serializes the value buffer per spec §3.2–3.3: all columns
    /// column-major (nulls occupy placeholder slots), then one trailing
    /// bitmap entry per column — a flag byte (1 = column has nulls) followed,
    /// when flagged, by a `rows/8 + 1`-byte LSB-first bitmap with bit=1 for
    /// null rows (the server's Java `BitMap` always allocates — and reads —
    /// `size/8 + 1` bytes, one extra padding byte when `rows % 8 == 0`).
    /// All multi-byte values big-endian.
    ///
    /// Sorts rows by timestamp first (spec §3.5).
    pub fn serialize_values(&mut self) -> Vec<u8> {
        self.sort_by_timestamp();
        let rows = self.row_count();
        let mut buf = Vec::new();
        for (col, &ty) in self.values.iter().zip(&self.types) {
            for cell in col {
                write_cell(&mut buf, ty, cell.as_ref());
            }
        }
        // Trailing per-column null bitmap section.
        for col in &self.values {
            let nulls: Vec<bool> = col.iter().map(Option::is_none).collect();
            if nulls.iter().any(|&n| n) {
                buf.push(1);
                // Pad to the server's fixed `rows/8 + 1` BitMap length.
                let mut bitmap = pack_bits_lsb_first(&nulls);
                bitmap.resize(rows / 8 + 1, 0);
                buf.extend_from_slice(&bitmap);
            } else {
                buf.push(0);
            }
        }
        debug_assert!(self.values.iter().all(|c| c.len() == rows));
        buf
    }

    /// Serializes the timestamp buffer per spec §3.4: `row_count` contiguous
    /// i64 big-endian values, no count prefix.
    ///
    /// Sorts rows by timestamp first (spec §3.5).
    pub fn serialize_timestamps(&mut self) -> Vec<u8> {
        self.sort_by_timestamp();
        let mut buf = Vec::with_capacity(self.timestamps.len() * 8);
        for ts in &self.timestamps {
            buf.extend_from_slice(&ts.to_be_bytes());
        }
        buf
    }
}

/// Writes one cell, using the C#-style sentinel placeholders for nulls
/// (spec §3.2 — any placeholder works, the server masks by bitmap).
fn write_cell(buf: &mut Vec<u8>, ty: TSDataType, cell: Option<&Value>) {
    match (ty, cell) {
        (TSDataType::Boolean, Some(Value::Boolean(b))) => buf.push(u8::from(*b)),
        (TSDataType::Boolean, None) => buf.push(0),
        (TSDataType::Int32, Some(Value::Int32(v))) => buf.extend_from_slice(&v.to_be_bytes()),
        (TSDataType::Int32, None) => buf.extend_from_slice(&i32::MIN.to_be_bytes()),
        (TSDataType::Int64, Some(Value::Int64(v))) => buf.extend_from_slice(&v.to_be_bytes()),
        (TSDataType::Timestamp, Some(Value::Timestamp(v))) => {
            buf.extend_from_slice(&v.to_be_bytes())
        }
        (TSDataType::Int64 | TSDataType::Timestamp, None) => {
            buf.extend_from_slice(&i64::MIN.to_be_bytes())
        }
        (TSDataType::Float, Some(Value::Float(v))) => buf.extend_from_slice(&v.to_be_bytes()),
        (TSDataType::Float, None) => buf.extend_from_slice(&f32::MIN.to_be_bytes()),
        (TSDataType::Double, Some(Value::Double(v))) => buf.extend_from_slice(&v.to_be_bytes()),
        (TSDataType::Double, None) => buf.extend_from_slice(&f64::MIN.to_be_bytes()),
        (TSDataType::Text, Some(Value::Text(s))) | (TSDataType::String, Some(Value::String(s))) => {
            write_binary(buf, s.as_bytes());
        }
        (TSDataType::Blob, Some(Value::Blob(b))) => write_binary(buf, b),
        (TSDataType::Text | TSDataType::String | TSDataType::Blob, None) => write_binary(buf, &[]),
        // Null sentinel 10000101 = 1000-01-01 (yyyyMMdd), per C#/Java.
        (TSDataType::Date, Some(Value::Date(v))) => buf.extend_from_slice(&v.to_be_bytes()),
        (TSDataType::Date, None) => buf.extend_from_slice(&10000101i32.to_be_bytes()),
        // add_row validates cell types; Vector/Unknown are not insertable.
        (ty, cell) => unreachable!("cell {cell:?} does not match tablet column type {ty:?}"),
    }
}

/// 4-byte big-endian length prefix + raw bytes (TEXT/STRING/BLOB, §3.2).
fn write_binary(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    buf.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree_tablet(types: Vec<TSDataType>) -> Tablet {
        let measurements = (0..types.len()).map(|i| format!("s{i}")).collect();
        Tablet::new("root.sg.d1", measurements, types).unwrap()
    }

    #[test]
    fn int32_column_with_null_known_bytes() {
        let mut t = tree_tablet(vec![TSDataType::Int32]);
        t.add_row(1, vec![Some(Value::Int32(5))]).unwrap();
        t.add_row(2, vec![None]).unwrap();
        let expected = [
            0x00, 0x00, 0x00, 0x05, // row 0: 5
            0x80, 0x00, 0x00, 0x00, // row 1: i32::MIN placeholder
            0x01, // bitmap flag: has nulls
            0x02, // LSB-first: row 1 null → bit 1 → 0x02
        ];
        assert_eq!(t.serialize_values(), expected);
        assert_eq!(
            t.serialize_timestamps(),
            [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]
        );
    }

    #[test]
    fn no_nulls_writes_zero_flag_only() {
        let mut t = tree_tablet(vec![TSDataType::Boolean]);
        t.add_row(1, vec![Some(Value::Boolean(true))]).unwrap();
        t.add_row(2, vec![Some(Value::Boolean(false))]).unwrap();
        assert_eq!(t.serialize_values(), [0x01, 0x00, 0x00]);
    }

    #[test]
    fn all_types_known_bytes() {
        let types = vec![
            TSDataType::Boolean,
            TSDataType::Int32,
            TSDataType::Int64,
            TSDataType::Float,
            TSDataType::Double,
            TSDataType::Text,
            TSDataType::Timestamp,
            TSDataType::Date,
            TSDataType::Blob,
            TSDataType::String,
        ];
        let mut t = tree_tablet(types);
        t.add_row(
            100,
            vec![
                Some(Value::Boolean(true)),
                Some(Value::Int32(7)),
                Some(Value::Int64(-2)),
                Some(Value::Float(1.5)),
                Some(Value::Double(-0.5)),
                Some(Value::Text("ab".into())),
                Some(Value::Timestamp(3)),
                Some(Value::Date(20260710)),
                Some(Value::Blob(vec![0xDE, 0xAD])),
                Some(Value::String("é".into())),
            ],
        )
        .unwrap();

        let mut expected: Vec<u8> = Vec::new();
        expected.push(0x01); // true
        expected.extend_from_slice(&7i32.to_be_bytes());
        expected.extend_from_slice(&(-2i64).to_be_bytes());
        expected.extend_from_slice(&[0x3F, 0xC0, 0x00, 0x00]); // 1.5f32 IEEE 754 BE
        expected.extend_from_slice(&(-0.5f64).to_be_bytes());
        expected.extend_from_slice(&[0, 0, 0, 2, b'a', b'b']);
        expected.extend_from_slice(&3i64.to_be_bytes());
        expected.extend_from_slice(&20260710i32.to_be_bytes());
        expected.extend_from_slice(&[0, 0, 0, 2, 0xDE, 0xAD]);
        expected.extend_from_slice(&[0, 0, 0, 2, 0xC3, 0xA9]); // "é" UTF-8
        expected.extend_from_slice(&[0; 10]); // 10 columns, no nulls
        assert_eq!(t.serialize_values(), expected);
    }

    #[test]
    fn all_types_null_placeholders() {
        let types = vec![
            TSDataType::Boolean,
            TSDataType::Int32,
            TSDataType::Int64,
            TSDataType::Float,
            TSDataType::Double,
            TSDataType::Text,
            TSDataType::Timestamp,
            TSDataType::Date,
            TSDataType::Blob,
            TSDataType::String,
        ];
        let n = types.len();
        let mut t = tree_tablet(types);
        t.add_row(1, vec![None; n]).unwrap();

        let mut expected: Vec<u8> = Vec::new();
        expected.push(0x00); // false
        expected.extend_from_slice(&i32::MIN.to_be_bytes());
        expected.extend_from_slice(&i64::MIN.to_be_bytes());
        expected.extend_from_slice(&f32::MIN.to_be_bytes());
        expected.extend_from_slice(&f64::MIN.to_be_bytes());
        expected.extend_from_slice(&[0, 0, 0, 0]); // empty text
        expected.extend_from_slice(&i64::MIN.to_be_bytes());
        expected.extend_from_slice(&10000101i32.to_be_bytes()); // 1000-01-01
        expected.extend_from_slice(&[0, 0, 0, 0]); // empty blob
        expected.extend_from_slice(&[0, 0, 0, 0]); // empty string
        for _ in 0..n {
            expected.extend_from_slice(&[0x01, 0x01]); // flag + bitmap (row 0 null)
        }
        assert_eq!(t.serialize_values(), expected);
    }

    #[test]
    fn unsorted_rows_are_sorted_by_timestamp() {
        let mut t = tree_tablet(vec![TSDataType::Int32]);
        t.add_row(3, vec![Some(Value::Int32(30))]).unwrap();
        t.add_row(1, vec![None]).unwrap();
        t.add_row(2, vec![Some(Value::Int32(20))]).unwrap();

        let ts = t.serialize_timestamps();
        assert_eq!(
            ts,
            [1i64, 2, 3]
                .iter()
                .flat_map(|v| v.to_be_bytes())
                .collect::<Vec<u8>>()
        );
        // Values reordered with their rows; the null moved to row 0.
        let expected = [
            0x80, 0x00, 0x00, 0x00, // null placeholder (was ts=1)
            0x00, 0x00, 0x00, 0x14, // 20 (ts=2)
            0x00, 0x00, 0x00, 0x1E, // 30 (ts=3)
            0x01, 0x01, // flag + LSB-first bitmap: row 0 null
        ];
        assert_eq!(t.serialize_values(), expected);
        assert_eq!(t.timestamps(), &[1, 2, 3]);
    }

    #[test]
    fn sort_is_stable_for_equal_timestamps() {
        let mut t = tree_tablet(vec![TSDataType::Text]);
        t.add_row(5, vec![Some(Value::Text("first".into()))])
            .unwrap();
        t.add_row(1, vec![Some(Value::Text("zero".into()))])
            .unwrap();
        t.add_row(5, vec![Some(Value::Text("second".into()))])
            .unwrap();
        t.sort_by_timestamp();
        assert_eq!(t.timestamps(), &[1, 5, 5]);
        assert_eq!(t.values[0][1], Some(Value::Text("first".into())));
        assert_eq!(t.values[0][2], Some(Value::Text("second".into())));
    }

    #[test]
    fn bitmap_is_rows_over_8_plus_1() {
        // 8 rows with a null: the server's BitMap always reads rows/8 + 1
        // bytes, so an 8-row bitmap is 2 bytes (1 data + 1 padding).
        let mut t = tree_tablet(vec![TSDataType::Int32]);
        for i in 0..8 {
            let cell = if i == 7 { None } else { Some(Value::Int32(i)) };
            t.add_row(i64::from(i), vec![cell]).unwrap();
        }
        let buf = t.serialize_values();
        assert_eq!(buf.len(), 8 * 4 + 1 + 2);
        assert_eq!(buf[8 * 4], 0x01); // flag
        assert_eq!(buf[8 * 4 + 1], 0x80); // row 7 null, LSB-first → bit 7
        assert_eq!(buf[8 * 4 + 2], 0x00); // padding byte

        // 9 rows: still 2 bitmap bytes (9/8 + 1 = 2).
        let mut t = tree_tablet(vec![TSDataType::Int32]);
        for i in 0..9 {
            let cell = if i == 8 { None } else { Some(Value::Int32(i)) };
            t.add_row(i64::from(i), vec![cell]).unwrap();
        }
        let buf = t.serialize_values();
        assert_eq!(buf.len(), 9 * 4 + 1 + 2);
        assert_eq!(&buf[9 * 4..], [0x01, 0x00, 0x01]); // row 8 → byte 1 bit 0
    }

    #[test]
    fn multi_column_is_column_major() {
        let mut t = tree_tablet(vec![TSDataType::Int32, TSDataType::Boolean]);
        t.add_row(1, vec![Some(Value::Int32(1)), Some(Value::Boolean(true))])
            .unwrap();
        t.add_row(2, vec![Some(Value::Int32(2)), Some(Value::Boolean(false))])
            .unwrap();
        let expected = [
            0, 0, 0, 1, 0, 0, 0, 2, // int32 column, both rows
            1, 0, // boolean column, both rows
            0, 0, // both flags: no nulls
        ];
        assert_eq!(t.serialize_values(), expected);
    }

    #[test]
    fn empty_tablet_serializes_flags_only() {
        let mut t = tree_tablet(vec![TSDataType::Int64]);
        assert_eq!(t.serialize_values(), [0x00]);
        assert_eq!(t.serialize_timestamps(), Vec::<u8>::new());
    }

    #[test]
    fn table_model_tablet() {
        let t = Tablet::new_table(
            "sensors",
            vec!["tag1".into(), "f1".into(), "attr1".into()],
            vec![TSDataType::String, TSDataType::Double, TSDataType::String],
            vec![
                ColumnCategory::Tag,
                ColumnCategory::Field,
                ColumnCategory::Attribute,
            ],
        )
        .unwrap();
        assert!(t.is_table_model());
        assert_eq!(t.table_name(), "sensors");
        assert_eq!(
            t.column_categories().unwrap(),
            &[
                ColumnCategory::Tag,
                ColumnCategory::Field,
                ColumnCategory::Attribute
            ]
        );
    }

    #[test]
    fn constructor_and_add_row_validation() {
        assert!(Tablet::new("d", vec!["s1".into()], vec![]).is_err());
        assert!(Tablet::new_table(
            "t",
            vec!["c1".into()],
            vec![TSDataType::Int32],
            vec![ColumnCategory::Tag, ColumnCategory::Field],
        )
        .is_err());

        let mut t = tree_tablet(vec![TSDataType::Int32]);
        // Wrong arity.
        assert!(t.add_row(1, vec![]).is_err());
        // Wrong type.
        assert!(t.add_row(1, vec![Some(Value::Int64(1))]).is_err());
        // Explicit Value::Null behaves like None.
        t.add_row(1, vec![Some(Value::Null)]).unwrap();
        assert_eq!(&t.serialize_values()[4..], [0x01, 0x01]); // flag + null bitmap
        assert_eq!(t.row_count(), 1);
    }
}
