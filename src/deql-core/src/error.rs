use thiserror::Error as ThisError;

/// Error type for DeQL operations.
#[derive(ThisError, Debug)]
pub enum DeqlError {
    /// Database error.
    #[error("DbError# {0}")]
    DbError(String),

    /// Configuration error.
    #[error("ConfigError# {0}")]
    Config(String),

    /// Parse error.
    #[error("ParseError# {0}")]
    Parse(String),

    /// Not found.
    #[error("NotFound# {0}")]
    NotFound(String),

    /// Conflict.
    #[error("Conflict# {0}")]
    Conflict(String),

    /// Anyhow wrapper for other errors.
    #[error("OtherError# {0}")]
    Other(#[from] anyhow::Error),
}

/// Convenience result type with `DeqlError` as the error variant.
pub type Result<T, E = DeqlError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_format() {
        let err = DeqlError::NotFound("item-1".to_string());
        assert_eq!(err.to_string(), "NotFound# item-1");

        let err = DeqlError::DbError("connection failed".to_string());
        assert_eq!(err.to_string(), "DbError# connection failed");

        let err = DeqlError::Parse("invalid syntax".to_string());
        assert_eq!(err.to_string(), "ParseError# invalid syntax");
    }

    #[test]
    fn test_from_anyhow() {
        let anyhow_err: anyhow::Error = anyhow::anyhow!("something broke");
        let deql_err: DeqlError = anyhow_err.into();
        assert!(matches!(deql_err, DeqlError::Other(_)));
    }

    #[test]
    fn test_result_type() {
        let ok: Result<()> = Ok(());
        assert!(ok.is_ok());

        let err: Result<()> = Err(DeqlError::NotFound("x".to_string()));
        assert!(err.is_err());
    }

    #[test]
    fn test_all_variants() {
        let variants = [
            (DeqlError::DbError("x".into()), "DbError# x"),
            (DeqlError::Config("y".into()), "ConfigError# y"),
            (DeqlError::Parse("z".into()), "ParseError# z"),
            (DeqlError::NotFound("w".into()), "NotFound# w"),
            (DeqlError::Conflict("v".into()), "Conflict# v"),
        ];
        for (err, expected) in variants {
            assert_eq!(err.to_string(), expected);
        }
    }
}
