// Copyright 2026 OpenObserve Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::sync::{Arc, LazyLock as Lazy};

use chrono::{TimeZone, Utc};
use config::meta::{bitvec::BitVec, stream::FileKey};
use hashbrown::HashMap;
use object_store::ObjectMeta;
use parking_lot::RwLock;

use super::{ACCOUNT_SEPARATOR, TRACE_ID_SEPARATOR};

type SegmentData = HashMap<String, Arc<BitVec>>;

static FILES: Lazy<RwLock<HashMap<String, Vec<ObjectMeta>>>> = Lazy::new(Default::default);
static SEGMENTS: Lazy<RwLock<HashMap<String, SegmentData>>> = Lazy::new(Default::default);

pub fn get(trace_id: &str) -> Result<Vec<ObjectMeta>, anyhow::Error> {
    let data = match FILES.read().get(trace_id) {
        Some(data) => data.clone(),
        None => return Err(anyhow::anyhow!("trace_id not found: {}", trace_id)),
    };
    Ok(data)
}

pub async fn set(trace_id: &str, schema_key: &str, format: &str, files: Vec<FileKey>) {
    let key = format!("{trace_id}/schema={schema_key}/format={format}");
    let mut values = Vec::with_capacity(files.len());
    let mut segment_data = HashMap::new();
    for file in files {
        let modified = Utc.timestamp_nanos(file.meta.max_ts * 1000);
        let file_name = if file.account.is_empty() {
            format!("/{}/{}/{}", key, TRACE_ID_SEPARATOR, file.key)
        } else {
            format!(
                "/{}/{}/{}/{}/{}",
                key, TRACE_ID_SEPARATOR, file.account, ACCOUNT_SEPARATOR, file.key
            )
        };
        values.push(ObjectMeta {
            location: file_name.into(),
            last_modified: modified,
            size: file.meta.compressed_size as u64,
            e_tag: None,
            version: None,
        });
        if let Some(bin_data) = file.segment_ids {
            segment_data.insert(file.key, bin_data);
        }
    }
    FILES.write().insert(key.clone(), values);
    SEGMENTS.write().insert(key, segment_data);
}

pub fn clear(trace_id: &str) {
    // Remove all files for the given trace_id
    let r = FILES.read();
    let keys = r
        .keys()
        .filter(|x: &&String| x.starts_with(trace_id))
        .cloned()
        .collect::<Vec<_>>();
    drop(r);
    let mut w = FILES.write();
    for key in keys.iter() {
        w.remove(key);
    }
    w.shrink_to_fit();
    drop(w);

    // Remove all segment data for the given trace_id
    let mut w = SEGMENTS.write();
    for key in keys.iter() {
        w.remove(key);
    }
    w.shrink_to_fit();
    drop(w);
}

pub fn get_segment_ids(file_key: &str) -> Option<Arc<BitVec>> {
    let (trace_id, filename) = file_key.split_once("/$$/")?;
    let segs = SEGMENTS.read();
    let seg_bv = segs
        .get(trace_id)
        .and_then(|data: &SegmentData| data.get(filename))
        .cloned();

    #[cfg(feature = "deql")]
    {
        let wal_segs = WAL_SEGMENTS.read();
        let wal_bv = wal_segs
            .get(trace_id)
            .and_then(|data: &SegmentData| data.get(filename))
            .cloned();
        match (seg_bv, wal_bv) {
            (Some(seg), Some(wal)) => {
                // Merge: OR the two BitVecs (must be same length)
                if seg.len() == wal.len() {
                    let mut merged = (*seg).clone();
                    for (i, bit) in wal.iter().enumerate() {
                        if *bit {
                            merged.set(i, true);
                        }
                    }
                    Some(Arc::new(merged))
                } else {
                    Some(seg)
                }
            }
            (Some(seg), None) => Some(seg),
            (None, Some(wal)) => Some(wal),
            (None, None) => None,
        }
    }

    #[cfg(not(feature = "deql"))]
    {
        seg_bv
    }
}

// --- DeQL WAL segment support ---

#[cfg(feature = "deql")]
static WAL_SEGMENTS: Lazy<RwLock<HashMap<String, SegmentData>>> = Lazy::new(Default::default);

/// Register WAL segment BitVecs for a trace_id and filename.
#[cfg(feature = "deql")]
#[allow(dead_code)]
pub fn set_wal_segment_ids(trace_id: &str, filename: &str, bv: Arc<BitVec>) {
    let mut w = WAL_SEGMENTS.write();
    let entry = w.entry(trace_id.to_string()).or_default();
    entry.insert(filename.to_string(), bv);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_returns_error_for_missing_trace() {
        let result = get("nonexistent_trace");
        assert!(result.is_err());
    }

    #[cfg(feature = "deql")]
    #[test]
    fn test_wal_segment_ids_merge() {
        let trace_id = "test_trace_merge";
        let filename = "file1.parquet";
        let mut seg = BitVec::repeat(false, 10);
        seg.set(2, true);
        seg.set(5, true);
        let mut wal = BitVec::repeat(false, 10);
        wal.set(5, true); // overlap
        wal.set(7, true); // unique to WAL
        SEGMENTS.write().insert(trace_id.to_string(), {
            let mut m = HashMap::new();
            m.insert(filename.to_string(), Arc::new(seg));
            m
        });
        set_wal_segment_ids(trace_id, filename, Arc::new(wal));
        let merged = get_segment_ids(&format!("{}/$$/{}", trace_id, filename)).unwrap();
        assert_eq!(merged.get(2).map(|b| *b), Some(true));
        assert_eq!(merged.get(5).map(|b| *b), Some(true));
        assert_eq!(merged.get(7).map(|b| *b), Some(true));
        assert_eq!(merged.get(0).map(|b| *b), Some(false));
    }
}
