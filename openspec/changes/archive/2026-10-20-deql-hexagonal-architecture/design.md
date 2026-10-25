# Design

## Context

OpenObserve is a monorepo of 35+ workspace members built on Axum, DataFusion, and SeaORM. The root `Cargo.toml` controls feature flags that propagate downstream. DeQL is a new subsystem (domain-event event-sourcing with SQL decision logic) that must be opt-in and zero-cost when disabled. The deql-cli project already uses a multi-crate architecture with pure logic separated from runtime adapters — this design reuses that pattern.

## Goals / Non-Goals

**Goals:**
- Establish a compile-time boundary between pure domain logic and infrastructure code using Cargo's dependency graph.
- Define port traits in `openobserve-deql-core` that abstract all OpenObserve-facing capabilities.
- Provide adapter implementations in `openobserve-deql` wired to SeaORM, DataFusion, and Axum.
- Gate all integration points behind a single `deql` feature flag with five bootstrap hooks.
- Keep the default build (no features) identical to the current binary — no DeQL symbols, zero code bloat.

**Non-Goals:**
- Implementing actual DeQL query semantics (parser, command execution logic).
- Writing integration tests or end-to-end tests.
- Migration from the old deql-cli implementation.
- Changing any existing workspace member's behavior.

## Decisions

### 1. Crate layout

Two workspace members under `src/`, mirrors deql-cli's separation:

| Crate | Workspace path | Purpose | `Cargo.toml` name |
|-------|---------------|---------|-------------------|
| `openobserve-deql-core` | `src/deql-core` | Pure domain logic, zero O2 infrastructure deps | `openobserve-deql-core` |
| `openobserve-deql` | `src/deql` | Infrastructure adapters, HTTP handlers | `openobserve-deql` |

`openobserve-deql-core` has no `[dependencies]` that point to other workspace members or any OpenObserve crate. It depends only on third-party crates that are itself-purpose: `serde`, `thiserror`, `chrono`, `uuid`, `regex`, `datafusion` (for SQL types only — no DataFusion session/catalog).

`openobserve-deql` depends on `openobserve-deql-core` (for traits) plus `sea-orm`, `datafusion`, `axum`, and any other infra crates needed for concrete implementations.

``` 
src/deql-core/src/
├── lib.rs              # Re-exports: parser, error, registry, context, event_id, concept
├── concept.rs          # ConceptKind enum
├── context.rs          # ExecutionContext trait + SqlResult/PreparedStatement types
├── error.rs            # DeqlError enum + Result<T> alias
├── event_id.rs         # EventIdProvider trait
├── event_store.rs      # EventStore trait + Event/AuditEntry types
├── parser/
│   └── mod.rs          # parse_events() stub
└── registry/
    ├── mod.rs          # Registry trait + InMemoryRegistry
    ├── entry.rs        # RegistryEntry struct
    └── in_memory.rs    # InMemoryRegistry impl (HashMap-backed)

src/deql/src/
├── lib.rs              # Re-exports: bootstrap::init, DeqlConfig
├── bootstrap.rs        # init() — entry/exit logging, calls init_registry/store/context/workers/df
├── config.rs           # DeqlConfig struct
├── handlers/
│   ├── mod.rs          # re-export routes
│   ├── config.rs       # DeqlConfig (adapter-level)
│   └── routes.rs       # deql_routes() → axum::Router at /api/deql
├── context/
│   ├── mod.rs          # re-export DataFusionContext
│   └── datafusion.rs   # DataFusionContext impl ExecutionContext
├── event_id/
│   ├── mod.rs          # re-export UuidEventIdProvider
│   └── provider.rs     # UuidEventIdProvider impl EventIdProvider
├── event_store/
│   ├── mod.rs          # re-export SeaOrmEventStore
│   └── sea_orm.rs      # SeaOrmEventStore impl EventStore
└── registry/
    ├── mod.rs          # re-export SeaOrmRegistry
    └── sea_orm.rs      # SeaOrmRegistry impl Registry
```

### 2. Feature flag design

A single `deql` feature at the root `Cargo.toml` makes the adapter crate available and gates every integration point in one pass:

```toml
# Root Cargo.toml [features]
deql = ["dep:openobserve-deql", "common/deql", "openobserve-api-http/deql"]
```

`openobserve-deql` is added to the root `[dependencies]` as an optional crate:

```toml
# Root Cargo.toml [dependencies]
openobserve-deql = { path = "src/deql", optional = true }
```

Feature propagation ensures the `common/deql` and `openobserve-api-http/deql` features activate the gated modules in those workspace members. The `deql = []` no-op features in `common/Cargo.toml` and `api/http/Cargo.toml` exist solely to accept the parent's feature enablement. `#[cfg(feature = "deql")]` gates every integration point:

| Bootstrap hook | File | What it registers |
|---------------|------|-------------------|
| Route registration | `src/api/http/src/handler/http/router/mod.rs` | `app.nest("/api/deql", openobserve_deql::handlers::deql_routes(cfg))` behind `#[cfg(feature = "deql")]` |
| Infra init | `src/main.rs` | `openobserve_deql::bootstrap::init()` behind `#[cfg(feature = "deql")]` |
| Migration | `src/main.rs` | DeQL migrations stub behind `#[cfg(feature = "deql")]` |
| Job registration | `src/main.rs` | DeQL background workers stub behind `#[cfg(feature = "deql")]` |
| Meta types | `src/common/src/meta/mod.rs` | `pub mod deql;` behind `#[cfg(feature = "deql")]` |

### 3. Port trait design

Four traits define the contract between core and adapters. All are `pub trait ... + Send + Sync + 'static` for dynamic dispatch via `Arc<dyn Trait>`.

| Trait | Location | Methods | Purpose |
|-------|----------|---------|---------|
| `Registry` | `deql-core/src/registry/mod.rs` | `insert`, `get`, `list`, `remove`, `upsert_many`, `export_all` | Event concept registry (CRUD for domain concepts) |
| `EventStore` | `deql-core/src/event_store.rs` | `append_event`, `append_audit`, `events_for`, `export_events` | Append-only event persistence |
| `ExecutionContext` | `deql-core/src/context.rs` | `execute_sql`, `prepare_statement` | SQL execution abstraction (DataFusion-backed) |
| `EventIdProvider` | `deql-core/src/event_id.rs` | `next_id` | Monotonic event ID generation |

| Adapter | Location | Trait impl'd | Backend |
|---------|----------|-------------|---------|
| `SeaOrmRegistry` | `deql/src/registry/sea_orm.rs` | `Registry` | SeaORM |
| `SeaOrmEventStore` | `deql/src/event_store/sea_orm.rs` | `EventStore` | SeaORM |
| `DataFusionContext` | `deql/src/context/datafusion.rs` | `ExecutionContext` | DataFusion |
| `UuidEventIdProvider` | `deql/src/event_id/provider.rs` | `EventIdProvider` | uuid v4 |

### 4. Dependency direction

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  OpenObserve    │────▶│ openobserve-deql │────▶│ openobserve-     │
│  (root crate)   │     │ (adapters)       │     │ deql-core        │
│                 │     │                  │     │ (pure domain)    │
│ • api/http      │     │ • SeaORM impl    │     │ • parser         │
│ • infra         │     │ • DataFusion impl│     │ • error types    │
│ • search        │     │ • Axum handlers  │     │ • Registry trait │
│ • ingester      │     │ • bootstrap()    │     │ • EventStore trt │
│ • jobs          │     └──────────────────┘     │ • ExecutionContext│
│                 │                               │ • EventIdProvider │
│                 │                               └──────────────────┘
└─────────────────┘
```

Dependency graph flows right-to-left: adapters depend on core, OpenObserve depends on adapters. Core has no workspace dependencies.

### 5. Parser isolation

The DSL parser (`deql-core/src/parser/`) is pure Rust with no external state. It consumes a SQL-like event discovery string and produces a `Concept` AST. This is testable in isolation with `cargo test -p openobserve-deql-core parser` — no DataFusion session, no database, no HTTP server needed.

Parser dependencies:
- `regex` for tokenization patterns
- `thiserror` for error types
- `serde` (optional, for serialization)

No DataFusion dependency needed for the parser if it produces its own AST types. If the parser reuses DataFusion SQL nodes directly, add `datafusion` as a dev-dependency only and write a thin adapter layer for runtime.

### 6. In-memory registry for testing

`openobserve-deql-core` ships a `InMemoryRegistry` backed by `HashMap` that implements the `Registry` trait. This enables tests to exercise domain logic without SeaORM. The trait + impl coexist in the same crate:

```rust
// deql-core/src/registry/mod.rs
pub trait Registry: Send + Sync + 'static {
    async fn insert(&self, entry: RegistryEntry) -> Result<(), DeqlError>;
    async fn get(&self, id: &str) -> Result<Option<RegistryEntry>, DeqlError>;
    // ... etc
}

pub struct InMemoryRegistry { ... }
impl Registry for InMemoryRegistry { ... }
```

### 7. Bootstrap function signature

The `openobserve-deql` crate exports a single `init()` function that returns a future. OpenObserve's bootstrap pipeline calls it inside `#[cfg(feature = "deql")]` blocks in `src/main.rs`:

```rust
// src/deql/src/bootstrap.rs
pub async fn init(cfg: &DeqlConfig) -> Result<(), anyhow::Error> {
    info!(target: "deql", "[deql] starting deql subsystem");

    if !cfg.enabled {
        info!(target: "deql", "[deql] deql subsystem skipped (disabled)");
        return Ok(());
    }

    init_registry().await?;
    init_event_store().await?;
    init_context().await?;
    spawn_workers().await?;
    prepare_datafusion().await?;

    info!(target: "deql", "[deql] deql subsystem completed");
    Ok(())
}
```

The `DeqlConfig` struct lives in `deql/src/config.rs` (adapter crate, not core) because it references `openobserve`'s config (`cfg.common.deql_enabled`). Each integration file passes the appropriate config value.

#### Error types

`openobserve-deql-core` defines a single `DeqlError` enum using `thiserror`, matching O2's pattern (`infra/src/errors/mod.rs`): uses `#[error("Prefix# {0}")]` display format, `#[from]` for transparent conversion, and a `pub type Result<T>` alias.

```rust
// deql-core/src/error.rs
use thiserror::Error as ThisError;

#[derive(ThisError, Debug)]
pub enum DeqlError {
    #[error("DbError# {0}")]
    DbError(String),
    #[error("ConfigError# {0}")]
    Config(String),
    #[error("ParseError# {0}")]
    Parse(String),
    #[error("NotFound# {0}")]
    NotFound(String),
    #[error("Conflict# {0}")]
    Conflict(String),
    /// Anyhow wrapper — let other crate errors bubble through.
    #[error("OtherError# {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = DeqlError> = std::result::Result<T, E>;
```

Public functions in `deql-core` return `Result<T>` with `DeqlError`. Public functions in `openobserve-deql` (adapters) return `Result<T, anyhow::Error>` since they are application-level — they convert `DeqlError` via `?` and let infra errors propagate as anyhow.

Init functions called from `main.rs` follow the O2 convention: `-> Result<(), anyhow::Error>`, and the caller wraps failures with `panic!("deql {hook} failed: {e}")`.

#### Logging markers

Every integration hook logs a `[deql]`-prefixed message on entry and exit using the `log` crate (the same crate O2 uses everywhere, initialized by `env_logger`):

```rust
// Example: src/api/http/src/handler/http/router/mod.rs — route hook
#[cfg(feature = "deql")]
{
    log::info!("[deql] registering deql routes");
    let deql_cfg = openobserve_deql::DeqlConfig {
        enabled: cfg.common.deql_enabled,
    };
    app = app.nest(
        "/api/deql",
        openobserve_deql::handlers::deql_routes(&deql_cfg),
    );
}
```

Conventions:
- Prefix `[deql]` — groups all DeQL logs together in `env_logger` output; grep for it in startup logs to confirm DeQL was reached.
- "starting" / "completed" on every path — if a crash or fatal error lands between the two markers, you know exactly where init stopped.
- The `log` crate is already a transitive dependency of every O2 crate; no new dependency needed.

#### Error propagation

Each integration hook is called inside `#[cfg(feature = "deql")]` in `main.rs`'s `run()` function, following O2's established pattern:

```rust
// In src/main.rs — infra init hook
#[cfg(feature = "deql")]
if let Err(e) = openobserve_deql::bootstrap::init(&openobserve_deql::DeqlConfig {
    enabled: cfg.common.deql_enabled,
}).await {
    job_init_tx.send(false).ok();
    panic!("deql infra init failed: {e}");
}
```

The `#[cfg(feature = "deql")]` block is invisible when the feature is off — no symbol resolution, no runtime overhead, no compile cost.

## Risks / Trade-offs

| Risk | Impact | Mitigation |
|------|--------|------------|
| Parser needs DataFusion types | Violates "core has no O2 deps" | Keep parser AST independent; if DataFusion nodes are needed, put them in `deql` (adapter crate) not `deql-core` |
| `openobserve-deql::init()` needs `openobserve::config::CONFIG` | Adapter crate depends on root, not just core | Acceptable — `deql` is the adapter crate and may depend on any O2 crate. Core stays clean. |
| Five bootstrap hooks across five files | Scatter, hard to audit | Each hook is a single `mod deql;` + one function call — minimal diff per file, easy to review in isolation |
