# Proposal

## Why

The DeQL re-implementation needs a clean architectural boundary between pure domain logic (parser, error types, in-memory registry, command validation) and infrastructure concerns (SeaORM persistence, DataFusion execution, Axum HTTP, background workers). Without this boundary, infrastructure dependencies leak into domain code, making testing harder, increasing merge risk, and preventing the DeQL crate from being reasoned about independently of OpenObserve. The deql-cli source project already uses this pattern — multiple independent crates with pure logic separated from runtime adapters.

## What Changes

- Create two workspace members under `src/`: `openobserve-deql-core` (pure domain logic, zero OpenObserve dependencies) and `openobserve-deql` (adapters that implement core traits against OpenObserve infrastructure)
- Define port traits in `openobserve-deql-core`: `Registry`, `EventStore`, `ExecutionContext`
- Implement adapters in `openobserve-deql`: SeaORM-backed registry, DataFusion-backed execution context, Axum route handlers
- Add `deql` feature flag to root `Cargo.toml` and propagate through dependent workspace members
- Wire `#[cfg(feature = "deql")]` stubs into OpenObserve's router, init, migration, and job modules

## Capabilities

### New Capabilities

- `deql-architecture`: Hexagonal architecture for the DeQL subsystem — ports in `openobserve-deql-core`, adapters in `openobserve-deql`, with compile-time enforcement of the dependency boundary

### Modified Capabilities

<!-- None — no existing capabilities change requirements -->

## Impact

- New workspace members: `src/deql-core/`, `src/deql/`
- Root `Cargo.toml` gains single `deql = ["dep:openobserve-deql"]` feature; `openobserve-deql` added as optional dependency
- OpenObserve bootstrap pipeline gains `#[cfg(feature = "deql")]` integration hooks (init, migration, routes, jobs)
- No behavioral change to existing OpenObserve functionality when `deql` feature is disabled
