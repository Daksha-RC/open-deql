/// Kind of a concept in the DeQL domain model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ConceptKind {
    /// Placeholder kind until domain types are defined.
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_kind() {
        assert_eq!(ConceptKind::Unknown, ConceptKind::Unknown);
    }

    #[test]
    fn test_concept_kind_copy() {
        let kind = ConceptKind::Unknown;
        let _copied = kind; // verify Copy is implemented
        let _also_copied = kind;
    }

    #[test]
    fn test_concept_kind_debug() {
        let kind = ConceptKind::Unknown;
        let debug_str = format!("{:?}", kind);
        assert!(debug_str.contains("Unknown"));
    }

    #[test]
    fn test_concept_kind_clone() {
        let kind = ConceptKind::Unknown;
        let _cloned = kind; // Copy, not clone
        let _also_copied = kind;
    }
}
