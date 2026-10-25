use openobserve_deql_core::error::{DeqlError, Result};

/// Configuration for DeQL subsystem.
#[derive(Debug, Clone)]
pub struct DeqlConfig {
    /// Whether DeQL is enabled.
    pub enabled: bool,
    // Future fields can be added here (database URL, etc.)
}

impl DeqlConfig {
    /// Create a disabled config by default.
    pub fn disabled() -> Self {
        Self { enabled: false }
    }
}
