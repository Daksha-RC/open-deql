# Tasks

## 1. Workspace & Feature Flag Setup

- [x] 1.1 Add `openobserve-deql-core` and `openobserve-deql` to `[workspace.members]` in root `Cargo.toml` and verify `cargo metadata` lists them
- [x] 1.2 Add `thiserror`, `uuid`, `regex`, `chrono`, `serde`, `anyhow` to `[workspace.dependencies]` (if not already present) with versions matching existing usage
- [x] 1.3 Add `openobserve-deql = { path = "src/deql", optional = true }` to root `[dependencies]` and `deql = ["dep:openobserve-deql"]` to root `[features]`
- [x] 1.4 Verify `cargo build` (no features) still succeeds with no change to the output binary

## 2. Core Crate Scaffolding

- [x] 2.1 Create `src/deql-core/Cargo.toml` with workspace package, `thiserror`, `serde`, `chrono`, `uuid`, `regex`, `anyhow`, `tokio` (for `tokio::time` only in tests), no workspace deps, and verify `cargo check -p openobserve-deql-core` succeeds
- [x] 2.2 Create `src/deql-core/src/lib.rs` with module declarations for `error`, `registry`, `parser`, `event_store`, `context`, `event_id`, `concept`
- [x] 2.3 Create `src/deql-core/src/concept.rs` stub with `ConceptKind` enum (placeholder) and verify it compiles

## 3. Error Types

- [x] 3.1 Create `src/deql-core/src/error.rs` with `DeqlError` enum (`thiserror`, `"Prefix# {0}"` format, `#[from] anyhow::Error`), `pub type Result<T>`, and verify display output matches format
- [x] 3.2 Re-export `DeqlError` and `Result` from `lib.rs` and verify downstream `use openobserve_deql_core::{DeqlError, Result};` compiles

## 4. Port Traits

- [x] 4.1 Create `src/deql-core/src/registry/mod.rs` with `pub trait Registry: Send + Sync + 'static` and methods `insert`, `get`, `list`, `remove`, `upsert_many`, `export_all` — verify trait object dispatch compiles
- [x] 4.2 Create `src/deql-core/src/event_store.rs` with `pub trait EventStore: Send + Sync + 'static` and methods `append_event`, `append_audit`, `events_for`, `export_events` — verify trait object dispatch compiles
- [x] 4.3 Create `src/deql-core/src/context.rs` with `pub trait ExecutionContext: Send + Sync + 'static` and methods `execute_sql`, `prepare_statement` — verify trait object dispatch compiles
- [x] 4.4 Create `src/deql-core/src/event_id.rs` with `pub trait EventIdProvider: Send + Sync + 'static` and method `next_id` — verify trait object dispatch compiles
- [x] 4.5 Re-export all four traits from `lib.rs` and verify `let r: Arc<dyn Registry> = ...` compiles in a downstream crate

## 5. Domain Types & In-Memory Registry

- [x] 5.1 Create `src/deql-core/src/registry/entry.rs` with `RegistryEntry` struct (id, name, kind, version, payload fields) and verify serde round-trip compiles
- [x] 5.2 Implement `InMemoryRegistry` (HashMap-backed) for the `Registry` trait in `src/deql-core/src/registry/mod.rs` — verify all six trait methods work with `HashMap`
- [x] 5.3 Create `src/deql-core/src/parser/mod.rs` stub with `parse_events()` signature returning `Result<Vec<Concept>, DeqlError>` and `ConceptKind` type, verify it compiles

## 6. Core Unit Tests

- [x] 6.1 Add `#[cfg(test)] mod tests` at end of `src/deql-core/src/registry/mod.rs` with tests: insert, get, list, remove, upsert_many, export_all — verify `cargo test -p openobserve-deql-core registry` passes
- [x] 6.2 Add `#[cfg(test)] mod tests` at end of `src/deql-core/src/error.rs` with tests: Display format, From<anyhow> conversion, Result type — verify `cargo test -p openobserve-deql-core error` passes
- [x] 6.3 Add `#[cfg(test)] mod tests` at end of `src/deql-core/src/concept.rs` with ConceptKind tests — verify `cargo test -p openobserve-deql-core concept` passes

## 7. Adapter Crate Scaffolding

- [x] 7.1 Create `src/deql/Cargo.toml` with `openobserve-deql-core` as dependency, `sea-orm`, `datafusion`, `axum`, `tokio`, `log`, `anyhow`, no other workspace deps — verify `cargo check -p openobserve-deql` succeeds
- [x] 7.2 Create `src/deql/src/lib.rs` with module declarations for `registry`, `event_store`, `context`, `event_id`, `bootstrap`, `handlers`
- [x] 7.3 Verify `cargo check -p openobserve-deql` succeeds and the crate builds as a shared library (`crate-type = ["lib", "rlib"]`)

## 8. Adapter Implementations

- [x] 8.1 Create `src/deql/src/registry/sea_orm.rs` with `SeaOrmRegistry` implementing `Registry` trait using SeaORM — verify `impl Registry for SeaOrmRegistry` compiles and `cargo check -p openobserve-deql` passes
- [x] 8.2 Create `src/deql/src/event_store/sea_orm.rs` with `SeaOrmEventStore` implementing `EventStore` trait using SeaORM — verify compilation
- [x] 8.3 Create `src/deql/src/context/datafusion.rs` with `DataFusionContext` implementing `ExecutionContext` trait — verify compilation
- [x] 8.4 Create `src/deql/src/event_id/provider.rs` with `UuidEventIdProvider` (or sequence-based) implementing `EventIdProvider` — verify compilation

## 9. Axum HTTP Handlers

- [x] 9.1 Create `src/deql/src/handlers/routes.rs` with `pub fn deql_routes(cfg: &DeqlConfig) -> Router<...>` that nests under `/api/deql` — verify it compiles
- [x] 9.2 Create `src/deql/src/handlers/mod.rs` re-exporting `routes` — verify compilation

## 10. Bootstrap & Logging

- [x] 10.1 Create `src/deql/src/bootstrap.rs` with `pub async fn init(cfg: &DeqlConfig) -> Result<(), anyhow::Error>` that calls `init_registry`, `init_event_store`, `init_context`, `spawn_workers`, `prepare_datafusion` — verify compilation
- [x] 10.2 Add `[deql]`-prefixed `log::info!` "starting" / "completed" markers at entry and exit of `init()` — verify the log messages appear in `env_logger` output
- [x] 10.3 Add `#[cfg(feature = "deql")]` gate to each integration point (5 hooks) in OpenObserve: route registration, infra init, migration, job registration, meta types — verify `cargo build` (no features) succeeds unchanged

## 11. Build Verification

- [x] 11.1 Build without `--features deql` and confirm `cargo tree -p openobserve-deql-core` shows zero O2 infra crates and `nm` / `cargo objdump` finds no `openobserve_deql` or `openobserve_deql_core` symbols in the binary
- [x] 11.2 Build with `--features deql` and confirm all five `#[cfg(feature = "deql")]` integration points are present (route hook, infra init, migration, job hook, meta types)
- [x] 11.3 Verify `cargo test -p openobserve-deql-core` passes all unit tests without external services
- [x] 11.4 Verify `cargo clippy --workspace --all-targets -- -W clippy::too_many_lines -W clippy::cognitive_complexity -W clippy::excessive_nesting -D warnings` passes with no new warnings

## Workflow follow-up

- Run `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -W clippy::too_many_lines -W clippy::cognitive_complexity -W clippy::excessive_nesting -D warnings`
- Archive the change after implementation and review
