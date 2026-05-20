# Registry Population Flow: How In-Memory DeReg Gets Populated from Persisted Data

## Overview

The DeQL registry (`DeReg`) is populated from persisted data (`dereg_meta_store`) through two primary mechanisms:

1. **Rehydration** (`rehydrate_org`) - Loads all audit rows for an org and rebuilds the in-memory registry
2. **Projection Worker** (`run_projection_tick`) - Incrementally applies new rows to projection tables during normal operations

## Architecture Components

### 1. In-Memory Registry State
**File**: [src/deql/src/deql_state.rs](src/deql/src/deql_state.rs)  
**Lines**: 1-20

The global singleton registry:
```rust
lazy_static! {
    pub static ref DEQL_STATE: OrgDeRegMap = OrgDeRegMap::new();
}

pub fn get_deql_state() -> &'static OrgDeRegMap {
    &*DEQL_STATE
}
```

### 2. Registry Storage Structure
**File**: [src/deql/src/org_registry.rs](src/deql/src/org_registry.rs)  
**Lines**: 1-70

Per-org registry map that stores in-memory `DeReg` instances:
```rust
pub struct OrgDeRegMap {
    inner: Arc<tokio::sync::RwLock<HashMap<OrgId, Arc<RwLock<DeReg>>>>>,
}

impl OrgDeRegMap {
    /// Returns existing or initialises a new empty DeReg for the org.
    pub async fn get_or_init(&self, org_id: &str) -> Arc<RwLock<DeReg>> { ... }
    
    /// Replace the DeReg for a given org (used during replay-refresh).
    pub async fn replace(&self, org_id: &str, dereg: DeReg) { ... }
}
```

---

## Rehydration Flow (Primary Loading Mechanism)

### Entry Point: HTTP Handler
**File**: [src/handler/http/request/dereg/rehydrate.rs](src/handler/http/request/dereg/rehydrate.rs)  
**Lines**: 59-110

The rehydration is triggered via HTTP POST to `/{org_id}/dereg/rehydrate`:

```rust
#[cfg(feature = "deql")]
pub async fn trigger_rehydrate(Path(org_id): Path<String>) -> Response {
    let state = get_deql_state().await;
    
    // Check if already in progress (early guard)
    if state.lock_map.is_locked(&org_id).await {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error": "Rehydration already in progress..."})),
        ).into_response();
    }

    // Get database connection
    let db = Arc::new(ORM_CLIENT.get_or_init(connect_to_orm).await.clone());

    // Spawn background job
    tokio::spawn(async move {
        let service: Arc<dyn o2_deql::RehydrateService> = Arc::new(
            RehydrateServiceImpl::new(db, org_map, lock_map, rehydrate_state_map)
        );

        service.rehydrate_org(&org_id_clone, None, Some(&trace_id_clone)).await
    });
}
```

### Rehydration Service Implementation
**File**: [src/deql/src/rehydrate_impl.rs](src/deql/src/rehydrate_impl.rs)  
**Lines**: 1-250

#### Main Entry: `rehydrate_org()` method
**Lines**: 100-170

The rehydration process:
1. **Lock check** - Ensures only one rehydration per org at a time
2. **Audit log query** - Reads all rows from `dereg_meta_store` for the org
3. **Parse and apply** - Replays each statement into a temporary `DeReg`
4. **Atomic swap** - Replaces the org's in-memory registry
5. **Result storage** - Records metrics and status

#### Core Flow: `perform_rehydrate()` method
**Lines**: 200-290

```rust
async fn perform_rehydrate(
    &self,
    org_id: &str,
    since_id: Option<i64>,
    rehydrate_id: &str,
    start_time: chrono::DateTime<chrono::Utc>,
) -> Result<RehydrateResult, RehydrateError> {
    // 1. Query dereg_meta_store for org_id, ordered by id
    let audit_rows = dereg_meta_store::Entity::find()
        .filter(dereg_meta_store::Column::OrgId.eq(org_id))
        .filter(if let Some(since) = since_id {
            dereg_meta_store::Column::Id.gt(since)
        } else {
            dereg_meta_store::Column::Id.gte(0i64)
        })
        .order_by_asc(dereg_meta_store::Column::Id)
        .all(self.db.as_ref())
        .await?;

    debug!("Queried audit rows: total_rows = {}", audit_rows.len());

    // 2. Build temporary DeReg by replaying audit rows
    let mut temp_dereg = DeReg::new();
    let mut last_seq_id = since_id.unwrap_or(0);
    let mut rows_processed = 0i64;

    for row in audit_rows {
        // Parse statement using the DeQL parser
        let (parsed, diagnostics) = parse(&row.statement);

        if parsed.statements.is_empty() && !diagnostics.is_empty() {
            error!("Parse error in audit row: {:?}", diagnostics);
            return Err(RehydrateError::ParseError(...));
        }

        // Apply each statement to temporary DeReg
        for spanned_stmt in parsed.statements {
            if let Err(e) = temp_dereg.register_statement(&spanned_stmt.node) {
                error!("Failed to apply statement: {}", e);
                return Err(RehydrateError::ParseError(...));
            }
        }

        last_seq_id = row.id;
        rows_processed += 1;
    }

    // 3. Atomically swap: replace live DeReg for this org
    self.org_map.replace(org_id, temp_dereg).await;

    // 4. Return success result with watermark
    Ok(RehydrateResult::success(...))
}
```

### Key Points on Rehydration:
- **Database Query**: Queries all `dereg_meta_store` rows for the org, ordered by ID
- **Idempotent**: Can specify `since_id` to load only rows after a checkpoint
- **Isolation**: Uses org-level locks to prevent concurrent rehydrations
- **Timeout**: 30-minute maximum timeout per rehydration
- **Atomic Swap**: Uses `OrgDeRegMap::replace()` for atomic in-memory update

---

## Projection Worker Flow (Incremental Updates)

### Worker Entry Point
**File**: [src/deql/src/projection_worker.rs](src/deql/src/projection_worker.rs)  
**Lines**: 39-77

The projection worker processes new rows incrementally:

```rust
pub async fn run_projection_tick(
    db: &DatabaseConnection,
    org_id: &str,
) -> Result<usize, sea_orm::DbErr> {
    // 1. Get current watermark (last processed row ID)
    let watermark = get_watermark(db, org_id).await?;

    // 2. Fetch new rows since watermark
    let rows = dereg_meta_store::Entity::find()
        .filter(dereg_meta_store::Column::OrgId.eq(org_id))
        .filter(dereg_meta_store::Column::Id.gt(watermark))
        .order_by_asc(dereg_meta_store::Column::Id)
        .all(db)
        .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    let max_id = rows.iter().map(|r| r.id).max().unwrap();
    let count = rows.len();

    // 3. Compute effective rows (resolve tombstones, latest per stream_id)
    let effective = compute_effective_rows(&rows);

    // 4. Apply in a transaction
    let txn = db.begin().await?;
    for eff in &effective {
        apply_effective_row(&txn, eff).await?;
    }

    // 5. Persist watermark (tracks progress)
    upsert_watermark(&txn, org_id, max_id).await?;
    txn.commit().await?;

    Ok(count)
}
```

**Note**: The projection worker populates **projection tables** (not the in-memory DeReg directly). It's a separate materialization layer for queries.

---

## Full Rebuild via Replay-Refresh

### Replay-Refresh: Manual Full Rebuild
**File**: [src/deql/src/replay.rs](src/deql/src/replay.rs)  
**Lines**: 103-145

Used to completely rebuild projection state from scratch:

```rust
pub async fn replay_refresh(
    db: &DatabaseConnection,
    org_id: &str,
    params: &ReplayRefreshParams,
) -> Result<ReplayRefreshResult, sea_orm::DbErr> {
    // Resolve target id
    let org_tip = get_org_tip_id(db, org_id).await?;

    let until_id = match (params.id, params.offset) {
        (Some(id), None) => id.min(org_tip),
        (None, Some(offset)) => (org_tip - offset).max(0),
        (None, None) => org_tip,
        (Some(_), Some(_)) => org_tip,
    };

    // Perform full rebuild from row 0 to until_id
    let replayed_until_id = apply_full_rebuild(db, org_id, until_id).await?;

    Ok(ReplayRefreshResult { ... })
}
```

### Full Rebuild Implementation
**File**: [src/deql/src/projection_worker.rs](src/deql/src/projection_worker.rs)  
**Lines**: 83-125

```rust
pub async fn apply_full_rebuild(
    db: &DatabaseConnection,
    org_id: &str,
    until_id: i64,
) -> Result<i64, sea_orm::DbErr> {
    // 1. Read all rows up to until_id
    let rows = dereg_meta_store::Entity::find()
        .filter(dereg_meta_store::Column::OrgId.eq(org_id))
        .filter(dereg_meta_store::Column::Id.lte(until_id))
        .order_by_asc(dereg_meta_store::Column::Id)
        .all(db)
        .await?;

    if rows.is_empty() {
        let txn = db.begin().await?;
        upsert_watermark(&txn, org_id, 0).await?;
        txn.commit().await?;
        return Ok(0);
    }

    let max_applied = rows.iter().map(|r| r.id).max().unwrap();

    // 2. Compute effective state using ALL rows (tombstone-aware)
    let effective = compute_effective_rows_full(&rows);

    let txn = db.begin().await?;

    // 3. Clear existing projections for this org
    clear_projections_for_org(&txn, org_id).await?;

    // 4. Apply effective rows
    for eff in &effective {
        apply_effective_row(&txn, eff).await?;
    }

    // 5. Persist watermark
    upsert_watermark(&txn, org_id, max_applied).await?;
    txn.commit().await?;

    Ok(max_applied)
}
```

---

## Validation-Only Replay

**File**: [src/deql/src/replay.rs](src/deql/src/replay.rs)  
**Lines**: 38-103

Read-only validation that doesn't mutate state:

```rust
pub async fn replay_validate(
    db: &DatabaseConnection,
    org_id: &str,
) -> Result<ReplayResult, sea_orm::DbErr> {
    // Read all rows for org
    let rows = dereg_meta_store::Entity::find()
        .filter(dereg_meta_store::Column::OrgId.eq(org_id))
        .order_by_asc(dereg_meta_store::Column::Id)
        .all(db)
        .await?;

    if rows.is_empty() {
        return Ok(ReplayResult { status: "ok", replayed: 0, errors: vec![] });
    }

    // Compute effective rows using full tombstone-aware logic
    let effective = compute_effective_rows_full(&rows);
    let mut errors = Vec::new();

    // Validate each effective row by re-parsing its statement
    let mut dereg = DeReg::new();
    for eff in &effective {
        if eff.is_tombstone {
            continue; // Tombstones are valid by definition
        }

        let (parsed, diagnostics) = parse(&eff.statement);

        if parsed.statements.is_empty() && !diagnostics.is_empty() {
            errors.push(format!("{}: parse error: {:?}", eff.stream_id, diagnostics));
            continue;
        }

        for spanned_stmt in &parsed.statements {
            if let Err(e) = dereg.register_statement(&spanned_stmt.node) {
                errors.push(format!("{}: {}", eff.stream_id, e));
            }
        }
    }

    Ok(ReplayResult { ... })
}
```

---

## Synchronization & Locking

### Org-Level Lock Map
**File**: [src/deql/src/worker_registry.rs](src/deql/src/worker_registry.rs)  
**Lines**: 65-120

Prevents concurrent rehydrations:

```rust
pub struct OrgLockMap {
    locks: Arc<RwLock<HashMap<OrgId, Arc<RwLock<()>>>>>,
}

impl OrgLockMap {
    pub async fn is_locked(&self, org_id: &str) -> bool {
        let locks = self.locks.read().await;
        if let Some(lock) = locks.get(org_id) {
            lock.try_read().is_err()  // Returns true if write-locked
        } else {
            false
        }
    }

    pub async fn acquire_write(&self, org_id: &str) -> OrgLockGuard {
        // Acquires exclusive write lock for the org
    }
}
```

---

## Startup Recovery Pattern

### Startup Recovery Test
**File**: [src/deql/src/metrics_tests.rs](src/deql/src/metrics_tests.rs)  
**Lines**: 149-187

Example of how startup recovery works:

```rust
#[tokio::test]
async fn startup_recovery_rehydrates_from_db() {
    // Simulate rows that existed before restart
    insert_row(&db, 1, "org1", "aggregate:Account", "AggregateCreated", 
               "AGGREGATE", 1, "ok", "CREATE AGGREGATE Account;", ...);
    insert_row(&db, 2, "org1", "command:OpenAccount", "CommandCreated", 
               "COMMAND", 2, "ok", "CREATE COMMAND OpenAccount;", ...);
    insert_row(&db, 3, "org1", "aggregate:Account", "AggregateDropped", 
               "AGGREGATE", 1, "ok", "DROP AGGREGATE Account;", ...);

    // On startup: read all rows for org ordered by id
    let rows = dereg_meta_store::Entity::find()
        .filter(dereg_meta_store::Column::OrgId.eq("org1"))
        .filter(dereg_meta_store::Column::Status.eq("ok"))
        .all(&db)
        .await
        .unwrap();

    // Build effective state: last row per stream_id wins
    let mut effective: HashMap<String, &dereg_meta_store::Model> = HashMap::new();
    for row in &rows {
        effective.insert(row.stream_id.clone(), row);
    }

    // After drop, Account stream has Dropped event_type, OpenAccount remains created
    let remaining: Vec<_> = effective.values()
        .filter(|r| !r.event_type.contains("Dropped"))
        .collect();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].stream_id, "command:OpenAccount");
}
```

---

## Data Flow Summary

```
┌─────────────────────────────┐
│   dereg_meta_store (DB)     │
│  (Audit log of all changes)  │
└──────────────┬──────────────┘
               │
      ┌────────┴────────┐
      │                 │
      ▼                 ▼
┌──────────────┐  ┌──────────────────┐
│ Rehydration  │  │ Projection       │
│ Service      │  │ Worker Ticks     │
│              │  │                  │
│ rehydrate_   │  │ run_projection_  │
│ org()        │  │ tick()           │
└──────┬───────┘  └────────┬─────────┘
       │                   │
       │          Populates projections:
       │          - meta_aggregates
       │          - meta_commands
       │          - meta_concepts
       │          - meta_decisions
       │          - meta_events
       │          - meta_inspections
       │          - meta_templates
       │          - meta_templates_instances
       │
       ▼
┌─────────────────────────────┐
│ In-Memory DeReg Instance    │
│ (OrgDeRegMap)               │
│                             │
│ - Per-org isolation         │
│ - Arc<RwLock<HashMap>>      │
│ - Stores DeReg per org_id   │
└─────────────────────────────┘
       │
       ▼
┌──────────────────────────────┐
│  Query/Execution Engine       │
│  (Schema provider, executor)  │
└──────────────────────────────┘
```

---

## Key Takeaways

1. **Rehydration is explicit** - Not automatic on startup; triggered via HTTP POST endpoint
2. **Idempotent checkpoint support** - Can resume from `since_id` to skip already-processed rows
3. **Two parallel paths**:
   - **Rehydration**: Rebuilds entire in-memory `DeReg` instance
   - **Projection Worker**: Incrementally builds projection tables (separate materialization)
4. **Atomic updates**: Uses `OrgDeRegMap::replace()` for thread-safe, atomic registry swaps
5. **Tombstone-aware**: Handles soft deletes via "tombstone" flags in `dereg_meta_store.meta`
6. **Watermarking**: Projection worker tracks progress via `projection_watermark` table
7. **Concurrency control**: Org-level locks prevent concurrent rehydrations
