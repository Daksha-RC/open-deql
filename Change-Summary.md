# Change Summary

## Overview
This commit introduces significant enhancements for DeQL (Data Engineering Query Language) functionality, primarily focused on improving inspection streams and query capabilities.

## Key Features Added

### DeQL Inspection Table Provider
- Added `InspectionTableProvider` that wraps in-memory RecordBatches
- Supports upsert operations (appending batches) for inspection output tables
- Enables querying inspection results via standard SQL through DataFusion's search API
- Thread-safe implementation using RwLock for concurrent access

### Enhanced Query Handling
- Added `deql_inspection_rewrite` module to detect and handle queries against inspection tables
- Added support for `deql_ins_*` and `deql_brn_*` stream queries
- Query resolution now bypasses normal storage backend and uses in-memory providers directly

### Startup Recovery Improvements
- Added `cleanup_ephemeral_streams` function to remove stale ephemeral streams on server startup
- Enhanced recovery process to clean up before rehydrating DeQL state
- Improved handling of virtual stream schemas during startup

### Service Architecture Updates
- Added new `src/service/deql` module with table provider implementation
- Modified `src/service/mod.rs` to include the new DeQL modules
- Updated `src/service/search/mod.rs` to integrate DeQL inspection query handling

### API Enhancements
- Added `decision_name` field to `ExecutionParams` struct
- Updated inspection state management with table provider registration and retrieval
- Enhanced memory tracking for inspection tables

## Technical Improvements

- **Memory Management**: Added memory byte tracking for inspection tables to monitor resource usage
- **Query Optimization**: Direct query execution against in-memory data without needing to write to disk
- **Thread Safety**: Improved concurrent access handling in inspection state management
- **Ephemeral Stream Cleanup**: Prevents stale ephemeral streams from persisting across server restarts

## Integration Points

- The implementation integrates with existing DataFusion infrastructure
- Works with the `deql` feature flag
- Maintains backward compatibility while extending functionality
- Supports both inspection output tables (`deql_ins_*`) and branching tables (`deql_brn_*`)

## Impact

This work represents a significant step towards enabling real-time querying of DeQL inspection results through standard SQL syntax, making inspection data more accessible and actionable for users.