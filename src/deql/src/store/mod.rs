//! SeaORM entities for DeQL DeReg persistence (Phase 2).
//!
//! Covers the canonical audit log (`dereg_meta_store`), projection watermark,
//! and all projection tables (`meta_*`).

pub mod dereg_meta_store;
pub mod projection_watermark;
pub mod projections;

#[cfg(test)]
mod tests;
