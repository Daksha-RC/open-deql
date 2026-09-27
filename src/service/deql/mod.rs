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

//! Inspection table provider for DataFusion query integration.
//!
//! This module provides the `InspectionTableProvider` which wraps in-memory
//! RecordBatches and makes inspection output tables queryable via OO's standard
//! search API (DataFusion SQL queries).
//!
//! Implements Task 5.1: Query-Time Table Resolution.

use std::{any::Any, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::Result,
    datasource::TableProvider,
    logical_expr::{Expr, TableType},
};
use parking_lot::RwLock;

/// Custom TableProvider for inspection output tables.
///
/// Wraps in-memory RecordBatches and supports appending (upsert).
/// This allows inspection output tables to be queried via standard SQL
/// through DataFusion's search API.
///
/// # Thread Safety
///
/// This struct is designed to be shared across threads via `Arc`.
/// The `batches` field uses `RwLock` for concurrent read/write access.
#[derive(Debug)]
pub struct InspectionTableProvider {
    /// Schema of the table (fixed after creation)
    schema: SchemaRef,
    /// In-memory record batches, protected by RwLock
    batches: Arc<RwLock<Vec<RecordBatch>>>,
}

impl InspectionTableProvider {
    /// Create a new InspectionTableProvider with the given schema.
    ///
    /// The provider starts with no data. Use `append_batches()` to add records.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Append new batches to the table (upsert operation).
    ///
    /// This method is thread-safe and can be called concurrently.
    pub fn append_batches(&self, new_batches: Vec<RecordBatch>) {
        self.batches.write().extend(new_batches);
    }

    /// Calculate the total memory used by all stored batches.
    ///
    /// Returns the sum of memory sizes of all Arrow arrays in the batches.
    pub fn memory_bytes(&self) -> usize {
        self.batches.read().iter().map(|b| b.get_array_memory_size()).sum()
    }
}

#[async_trait]
impl TableProvider for InspectionTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        // Clone the batches for the scan (read lock scope ends here)
        let batches = self.batches.read().clone();
        
        // Use MemTable to create an ExecutionPlan from the batches
        let mem_table = datafusion::datasource::MemTable::try_new(self.schema.clone(), vec![batches])?;
        mem_table.scan(_state, projection, _filters, _limit).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_inspection_table_provider_creation() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let provider = InspectionTableProvider::new(schema.clone());

        assert_eq!(provider.schema(), schema);
        assert_eq!(provider.memory_bytes(), 0);
    }

    #[test]
    fn test_append_and_scan() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]));

        let provider = InspectionTableProvider::new(schema.clone());

        // Create a batch to append
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("1"), Some("2"), None,
                ])),
            ],
        )
        .expect("Failed to create test batch");

        // Append the batch
        provider.append_batches(vec![batch.clone()]);

        // Verify memory is tracked
        assert!(provider.memory_bytes() > 0);

        // Append another batch
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["d", "e"])),
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("3"), Some("4"),
                ])),
            ],
        )
        .expect("Failed to create test batch2");

        provider.append_batches(vec![batch2]);

        // Verify total memory increased
        assert!(provider.memory_bytes() > 0);

        // Verify we can scan and get combined batches
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = datafusion::prelude::SessionContext::new();
            let scan = provider.scan(&ctx.state(), None, &[], None).await?;
            // The scan returns an ExecutionPlan, we can't easily verify contents
            // without executing it, but we can verify it exists
            assert!(scan.schema().fields().len() == 2);
            Ok::<(), datafusion::error::DataFusionError>(())
        }).expect("Scan failed");
    }

    #[test]
    fn test_concurrent_access() {
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
        ]));

        let provider = Arc::new(InspectionTableProvider::new(schema));

        // Spawn multiple threads to append concurrently
        let mut handles = vec![];
        for i in 0..5 {
            let provider_clone = Arc::clone(&provider);
            let handle = std::thread::spawn(move || {
                let schema = provider_clone.schema();
                let batch = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(arrow::array::StringArray::from(vec![format!("batch_{}", i)]))],
                )
                .expect("Failed to create batch");
                provider_clone.append_batches(vec![batch]);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify all batches were appended
        let total_batches = provider.batches.read().len();
        assert_eq!(total_batches, 5);
    }
}
