//! Global DeQL state accessor.
//!
//! Provides singleton access to the org-scoped DeQL registry (`OrgDeRegMap`)
//! using lazy_static for thread-safe initialization on first access.

use lazy_static::lazy_static;
use crate::org_registry::OrgDeRegMap;

lazy_static! {
    /// Global DeQL state — org-scoped registry map.
    ///
    /// Initialized on first access. Safe to call from any thread/async context.
    pub static ref DEQL_STATE: OrgDeRegMap = OrgDeRegMap::new();
}

/// Get reference to the global DeQL state.
///
/// This is safe to call from any context (sync or async).
pub fn get_deql_state() -> &'static OrgDeRegMap {
    &*DEQL_STATE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deql_state_singleton() {
        let state1 = get_deql_state();
        let state2 = get_deql_state();
        
        // Should be the same reference
        assert_eq!(
            state1 as *const OrgDeRegMap,
            state2 as *const OrgDeRegMap
        );
    }
}
