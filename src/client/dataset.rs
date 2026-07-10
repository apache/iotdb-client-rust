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

//! Query result iteration — the SessionDataSet state machine (protocol
//! spec §4.3): drain the current TsBlock, decode the next cached block,
//! fetch more pages while `moreData`, then auto-close.

use std::collections::VecDeque;

use crate::client::session::{QueryHandle, Session};
use crate::data::tsblock::TsBlock;
use crate::data::value::Value;
use crate::error::{Error, Result};

/// One result row: the timestamp (`None` when the server set
/// `ignoreTimeStamp`, e.g. aggregations and SHOW statements) and one
/// [`Value`] per output column.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub timestamp: Option<i64>,
    pub values: Vec<Value>,
}

/// An iterable query result set.
///
/// Borrows the [`Session`] mutably for its whole lifetime: `fetchResultsV2`
/// and `closeOperation` must reach the node that owns the query id, so the
/// connection stays pinned to the result set until it is closed (spec
/// gotcha #13). The borrow also means no other statement can run on the
/// session while a result set is open — close (or drop) it first.
///
/// Not a `std::iter::Iterator` — [`SessionDataSet::next_row`] returns
/// `Result<Option<Row>>` so decode and fetch errors surface cleanly.
/// The query is closed automatically on exhaustion, on [`SessionDataSet::close`],
/// or on drop (best-effort, matching the other SDKs).
pub struct SessionDataSet<'a> {
    session: &'a mut Session,
    query_id: i64,
    statement: String,
    columns: Vec<String>,
    data_type_list: Vec<String>,
    ignore_time_stamp: bool,
    /// Output column ordinal → physical TsBlock column index; `-1` = time
    /// column; identity mapping when `None` (spec §4.2 field 17).
    column_index_map: Option<Vec<i32>>,
    /// Cached serialized TsBlocks not yet decoded.
    pending_blocks: VecDeque<Vec<u8>>,
    /// Currently decoded block and the next row to yield from it.
    current: Option<TsBlock>,
    row_index: usize,
    more_data: bool,
    closed: bool,
}

impl<'a> SessionDataSet<'a> {
    pub(crate) fn new(session: &'a mut Session, handle: QueryHandle) -> SessionDataSet<'a> {
        SessionDataSet {
            session,
            query_id: handle.query_id,
            statement: handle.statement,
            columns: handle.columns,
            data_type_list: handle.data_type_list,
            ignore_time_stamp: handle.ignore_time_stamp,
            column_index_map: handle.column_index2_ts_block_column_index_list,
            pending_blocks: handle.query_result.into(),
            current: None,
            row_index: 0,
            more_data: handle.more_data,
            closed: false,
        }
    }

    /// Output column names.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Output column type names as reported by the server
    /// (`"INT64"`, `"TEXT"`, …), parallel to [`SessionDataSet::columns`].
    pub fn column_types(&self) -> &[String] {
        &self.data_type_list
    }

    /// Whether the server flagged this result as having no meaningful
    /// timestamps (row timestamps are then `None`).
    pub fn ignore_time_stamp(&self) -> bool {
        self.ignore_time_stamp
    }

    /// Advance to the next row, fetching further pages as needed.
    /// Returns `Ok(None)` when exhausted (the query is then auto-closed).
    pub fn next_row(&mut self) -> Result<Option<Row>> {
        if self.closed {
            return Ok(None);
        }
        loop {
            // 1. Unread rows in the current block → yield the next one.
            if let Some(block) = &self.current {
                if self.row_index < block.position_count {
                    let row = self.assemble_row()?;
                    self.row_index += 1;
                    return Ok(Some(row));
                }
                self.current = None;
            }
            // 2. More cached blocks → decode the next non-empty one.
            if let Some(bytes) = self.pending_blocks.pop_front() {
                self.current = Some(TsBlock::decode(&bytes)?);
                self.row_index = 0;
                continue;
            }
            // 3. Server has more pages → fetch and refill.
            if self.more_data {
                let (blocks, more) = self.session.fetch_results(self.query_id, &self.statement)?;
                self.pending_blocks = blocks.into();
                self.more_data = more;
                if !self.pending_blocks.is_empty() {
                    continue;
                }
                if self.more_data {
                    // hasResultSet with an empty page and moreData still
                    // set — loop and fetch again rather than spin here.
                    continue;
                }
            }
            // 4. Exhausted → auto-close and report the end.
            self.close();
            return Ok(None);
        }
    }

    /// Build the output row at `self.row_index` of the current block,
    /// mapping output columns through `columnIndex2TsBlockColumnIndexList`
    /// (`-1` = time column; identity when absent — spec §5.4).
    fn assemble_row(&self) -> Result<Row> {
        let block = self.current.as_ref().expect("current block");
        let i = self.row_index;
        let mut values = Vec::with_capacity(self.columns.len());
        for ordinal in 0..self.columns.len() {
            let physical = match &self.column_index_map {
                Some(map) => *map.get(ordinal).ok_or_else(|| {
                    Error::Decode(format!(
                        "column index map has {} entries, need output column {ordinal}",
                        map.len()
                    ))
                })?,
                None => ordinal as i32,
            };
            if physical == -1 {
                values.push(Value::Timestamp(block.timestamps[i]));
                continue;
            }
            let column = usize::try_from(physical)
                .ok()
                .and_then(|p| block.columns.get(p))
                .ok_or_else(|| {
                    Error::Decode(format!(
                        "column index map points at TsBlock column {physical}, block has {}",
                        block.columns.len()
                    ))
                })?;
            values.push(column[i].clone());
        }
        let timestamp = (!self.ignore_time_stamp).then(|| block.timestamps[i]);
        Ok(Row { timestamp, values })
    }

    /// Close the query (best-effort `closeOperation`, errors swallowed) and
    /// release the session borrow's obligations. Idempotent; also invoked
    /// automatically on exhaustion and on drop.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.pending_blocks.clear();
        self.current = None;
        self.session.close_query(self.query_id);
    }
}

impl Drop for SessionDataSet<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::session::SessionConfig;
    use crate::data::tsblock::test_util::{header, int32_block, time_column};
    use crate::data::tsblock::{ENCODING_BINARY_ARRAY, ENCODING_INT32_ARRAY};
    use crate::data::TSDataType;

    /// A never-opened session: fetch_results errors, close_query is a no-op —
    /// exactly what the offline state-machine tests need.
    fn offline_session() -> Session {
        Session::new(SessionConfig::default())
    }

    fn handle(blocks: Vec<Vec<u8>>, more_data: bool) -> QueryHandle {
        QueryHandle {
            query_id: 1,
            statement: "SELECT s1 FROM root.sg.d1".into(),
            columns: vec!["root.sg.d1.s1".into()],
            data_type_list: vec!["INT32".into()],
            ignore_time_stamp: false,
            query_result: blocks,
            more_data,
            column_index2_ts_block_column_index_list: None,
        }
    }

    #[test]
    fn drains_rows_across_multiple_cached_blocks() {
        let mut session = offline_session();
        let blocks = vec![int32_block(&[1, 2], &[10, 20]), int32_block(&[3], &[30])];
        let mut ds = SessionDataSet::new(&mut session, handle(blocks, false));

        let mut rows = Vec::new();
        while let Some(row) = ds.next_row().unwrap() {
            rows.push(row);
        }
        assert_eq!(
            rows,
            vec![
                Row {
                    timestamp: Some(1),
                    values: vec![Value::Int32(10)]
                },
                Row {
                    timestamp: Some(2),
                    values: vec![Value::Int32(20)]
                },
                Row {
                    timestamp: Some(3),
                    values: vec![Value::Int32(30)]
                },
            ]
        );
        // Exhausted → auto-closed; further calls keep returning None.
        assert_eq!(ds.next_row().unwrap(), None);
    }

    #[test]
    fn empty_result_set_yields_none_immediately() {
        let mut session = offline_session();
        let mut ds = SessionDataSet::new(&mut session, handle(vec![], false));
        assert_eq!(ds.next_row().unwrap(), None);
    }

    #[test]
    fn empty_blocks_are_skipped() {
        let mut session = offline_session();
        let blocks = vec![int32_block(&[], &[]), int32_block(&[7], &[70])];
        let mut ds = SessionDataSet::new(&mut session, handle(blocks, false));
        let row = ds.next_row().unwrap().unwrap();
        assert_eq!(row.timestamp, Some(7));
        assert_eq!(ds.next_row().unwrap(), None);
    }

    #[test]
    fn more_data_on_dead_session_surfaces_fetch_error() {
        // moreData=true forces a fetch_results call, which fails on a
        // never-opened session — the error must propagate, not panic.
        let mut session = offline_session();
        let mut ds = SessionDataSet::new(&mut session, handle(vec![], true));
        assert!(ds.next_row().is_err());
    }

    #[test]
    fn ignore_time_stamp_omits_timestamps() {
        let mut session = offline_session();
        let mut h = handle(vec![int32_block(&[5], &[50])], false);
        h.ignore_time_stamp = true;
        let mut ds = SessionDataSet::new(&mut session, h);
        assert!(ds.ignore_time_stamp());
        let row = ds.next_row().unwrap().unwrap();
        assert_eq!(row.timestamp, None);
        assert_eq!(row.values, vec![Value::Int32(50)]);
    }

    #[test]
    fn column_index_map_with_time_column_and_duplicates() {
        // Physical block: one TEXT column. Output: [time, text, text again].
        let mut b = header(&[TSDataType::Text], 2, &[ENCODING_BINARY_ARRAY]);
        b.extend_from_slice(&time_column(&[100, 200]));
        b.push(0); // mayHaveNull
        for s in ["a", "b"] {
            b.extend_from_slice(&(s.len() as i32).to_be_bytes());
            b.extend_from_slice(s.as_bytes());
        }

        let mut session = offline_session();
        let h = QueryHandle {
            query_id: 9,
            statement: "SELECT time, tag, tag FROM t".into(),
            columns: vec!["time".into(), "tag".into(), "tag".into()],
            data_type_list: vec!["TIMESTAMP".into(), "TEXT".into(), "TEXT".into()],
            ignore_time_stamp: true,
            query_result: vec![b],
            more_data: false,
            column_index2_ts_block_column_index_list: Some(vec![-1, 0, 0]),
        };
        let mut ds = SessionDataSet::new(&mut session, h);
        assert_eq!(ds.columns(), ["time", "tag", "tag"]);
        assert_eq!(ds.column_types(), ["TIMESTAMP", "TEXT", "TEXT"]);

        let row = ds.next_row().unwrap().unwrap();
        assert_eq!(
            row.values,
            vec![
                Value::Timestamp(100),
                Value::Text("a".into()),
                Value::Text("a".into()),
            ]
        );
        let row = ds.next_row().unwrap().unwrap();
        assert_eq!(row.values[0], Value::Timestamp(200));
        assert_eq!(ds.next_row().unwrap(), None);
    }

    #[test]
    fn identity_mapping_when_map_absent() {
        let types = [TSDataType::Int32, TSDataType::Int32];
        let mut b = header(&types, 1, &[ENCODING_INT32_ARRAY, ENCODING_INT32_ARRAY]);
        b.extend_from_slice(&time_column(&[1]));
        for v in [11i32, 22] {
            b.push(0);
            b.extend_from_slice(&v.to_be_bytes());
        }

        let mut session = offline_session();
        let mut h = handle(vec![b], false);
        h.columns = vec!["s1".into(), "s2".into()];
        h.data_type_list = vec!["INT32".into(), "INT32".into()];
        let mut ds = SessionDataSet::new(&mut session, h);
        let row = ds.next_row().unwrap().unwrap();
        assert_eq!(row.values, vec![Value::Int32(11), Value::Int32(22)]);
    }

    #[test]
    fn out_of_range_map_entry_is_decode_error() {
        let mut session = offline_session();
        let mut h = handle(vec![int32_block(&[1], &[10])], false);
        h.column_index2_ts_block_column_index_list = Some(vec![5]);
        let mut ds = SessionDataSet::new(&mut session, h);
        assert!(matches!(ds.next_row(), Err(Error::Decode(_))));
    }

    #[test]
    fn corrupt_block_is_decode_error() {
        let mut session = offline_session();
        let mut ds = SessionDataSet::new(&mut session, handle(vec![vec![0, 1]], false));
        assert!(matches!(ds.next_row(), Err(Error::Decode(_))));
    }

    #[test]
    fn explicit_close_stops_iteration() {
        let mut session = offline_session();
        let blocks = vec![int32_block(&[1, 2], &[10, 20])];
        let mut ds = SessionDataSet::new(&mut session, handle(blocks, false));
        assert!(ds.next_row().unwrap().is_some());
        ds.close();
        ds.close(); // idempotent
        assert_eq!(ds.next_row().unwrap(), None);
    }
}
