# deql-architecture Specification

## Purpose

Defines the behavioral contract for the DeQL subsystem's hexagonal architecture: pure domain logic in `openobserve-deql-core` with trait-based ports, and infrastructure adapters in `openobserve-deql`, enforced at compile time by Cargo's dependency system.

## Requirements

### Requirement: deql-core has no O2 infrastructure dependencies

The `openobserve-deql-core` crate MUST NOT depend on any OpenObserve infrastructure crate. The following crates MUST NOT appear in its transitive dependency graph: `infra`, `db`, `search`, `search_service`, `ingester`, `wal`, `openobserve-core`, `openobserve-api-*`.

#### Scenario: Core Cargo.toml has no O2 dependencies

- **WHEN** a reviewer inspects `src/deql-core/Cargo.toml`
- **THEN** no entry under `[dependencies]` references `openobserve-`, `infra`, `search`, `ingester`, `wal`, `db`, `openobserve-core`, or `openobserve-api-*`

#### Scenario: Core compiles without infrastructure crates

- **WHEN** a developer runs `cargo check -p openobserve-deql-core` from the workspace root
- **THEN** the command succeeds with no errors

---

### Requirement: Core exposes port traits for infrastructure

The `openobserve-deql-core` crate MUST define `pub trait` interfaces that abstract all OpenObserve-facing capabilities. Any capability — persistence, SQL execution, HTTP — MUST be expressed through a trait in core, not through direct imports.

#### Scenario: Core defines Registry trait

- **WHEN** a developer reads `openobserve-deql-core/src/registry/mod.rs`
- **THEN** they find a `pub trait Registry` with methods: `insert`, `get`, `list`, `remove`, `upsert_many`, `export_all`

#### Scenario: Core defines EventStore trait

- **WHEN** a developer reads `openobserve-deql-core/src/event_store.rs`
- **THEN** they find a `pub trait EventStore` with methods: `append_event`, `append_audit`, `events_for`, `export_events`

#### Scenario: Core defines ExecutionContext trait

- **WHEN** a developer reads `openobserve-deql-core/src/context.rs`
- **THEN** they find a `pub trait ExecutionContext` with methods: `execute_sql`, `prepare_statement`

#### Scenario: Core defines EventIdProvider trait

- **WHEN** a developer reads `openobserve-deql-core/src/event_id.rs`
- **THEN** they find a `pub trait EventIdProvider` with method: `next_id`

#### Scenario: All port traits support dynamic dispatch

- **WHEN** code writes `let reg: Arc<dyn Registry> = Arc::new(MyRegistry);`
- **THEN** the code compiles (traits are `pub trait ... + Send + Sync + 'static`)

---

### Requirement: Adapter implements all port traits

The `openobserve-deql` crate MUST provide a concrete implementation for every port trait. Each implementation MUST live in a separate module under `openobserve-deql/src/`.

#### Scenario: All traits have an adapter implementation

- **WHEN** a reviewer inspects `openobserve-deql/src/` for `impl ... for` blocks
- **THEN** `Registry`, `EventStore`, `ExecutionContext`, and `EventIdProvider` each have at least one `impl` block

#### Scenario: Adapter compiles against core traits

- **WHEN** the adapter crate declares `openobserve-deql-core` as a dependency and implements all traits
- **THEN** `cargo check -p openobserve-deql` succeeds

---

### Requirement: Bootstrap hooks are feature-gated

OpenObserve's bootstrap pipeline MUST gate all DeQL integration at five points: route registration, infra init, migration, job registration, and meta types.

#### Scenario: Build without deql produces clean binary

- **WHEN** `cargo build` runs without `--features deql`
- **THEN** the build succeeds and the binary contains no symbols from `openobserve_deql` or `openobserve_deql_core`

#### Scenario: Build with deql wires all hooks

- **WHEN** `cargo build --features deql` runs
- **THEN** all five integration points contain active `#[cfg(feature = "deql")]` code

#### Scenario: Feature flag propagates through workspace

- **WHEN** a workspace member declares `deql = ["dep:openobserve-deql"]` in `[features]`
- **THEN** enabling `deql` makes `openobserve-deql` importable behind `#[cfg(feature = "deql")]`

---

### Requirement: Core types are testable without infrastructure

Pure domain logic types in `openobserve-deql-core` MUST be testable without a running database, DataFusion session, or HTTP server. Tests MUST NOT depend on external services.

#### Scenario: Parser tests run standalone

- **WHEN** `cargo test -p openobserve-deql-core parser` runs
- **THEN** all parser tests pass without any external service

#### Scenario: In-memory registry tests run standalone

- **WHEN** `cargo test -p openobserve-deql-core registry` runs
- **THEN** tests pass using only `HashMap`-backed implementations shipped with `openobserve-deql-core`

---

### Requirement: Organization mirrors deql-cli patterns

The `openobserve-deql-core` and `openobserve-deql` crates SHALL mirror deql-cli's crate-to-module mapping.

| deql-cli crate | OpenObserve equivalent |
| `deql-parser` | `deql-core/src/parser/` |
| `deql-core` | `deql-core/src/` (error types, ConceptKind) |
| `deql-dereg` | `deql-core/src/registry/` + `deql/src/sea_orm_registry.rs` |
| `deql-dereg/executor.rs` | `deql/src/datafusion_context.rs` |
| `deql-server` | `deql/src/axum_handler.rs` |

#### Scenario: Core mirrors deql-cli domain structure

- **WHEN** a developer inspects `openobserve-deql-core/src/`
- **THEN** they find modules for `parser/`, `error.rs`, and `registry/` matching deql-cli's `deql-parser`, `deql-core`, and `deql-dereg`

#### Scenario: Adapter mirrors deql-cli runtime structure

- **WHEN** a developer inspects `openobserve-deql/src/`
- **THEN** they find adapter modules for SeaORM, DataFusion, and Axum matching deql-cli's `deql-dereg` and `deql-server`
