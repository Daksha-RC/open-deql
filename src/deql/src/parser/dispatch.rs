// Statement routing: DeQL vs SQL classification.

/// Classification of a statement as DeQL or SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    Deql,
    Sql,
}

/// Classify a statement by peeking at the first few tokens.
/// Does not consume or modify the input.
pub fn classify_statement(input: &str) -> StatementKind {
    let trimmed = strip_leading_comments(input.trim_start());
    // Take up to 80 chars for comparison to handle "CREATE OR REPLACE EVENTSTORE"
    let len = trimmed.len().min(80);
    let upper: String = trimmed[..len].to_uppercase();

    // Handle CREATE [OR REPLACE] <concept>
    if starts_with_word(&upper, "CREATE") {
        let rest = &upper["CREATE".len()..];
        let rest = skip_whitespace(rest);

        // Skip optional OR REPLACE
        let rest = if starts_with_word(rest, "OR") {
            let after_or = skip_whitespace(&rest["OR".len()..]);
            if starts_with_word(after_or, "REPLACE") {
                skip_whitespace(&after_or["REPLACE".len()..])
            } else {
                rest
            }
        } else {
            rest
        };

        if starts_with_deql_concept(rest) {
            return StatementKind::Deql;
        }
        return StatementKind::Sql;
    }

    // Single-word DeQL prefixes
    if starts_with_word(&upper, "EXPORT")
        || starts_with_word(&upper, "VALIDATE")
        || starts_with_word(&upper, "EXECUTE")
        || starts_with_word(&upper, "INSPECT")
        || starts_with_word(&upper, "DESCRIBE")
        || starts_with_word(&upper, "APPLY")
    {
        return StatementKind::Deql;
    }

    StatementKind::Sql
}

/// Check if `s` starts with `word` followed by a non-alphanumeric/underscore char (or end of string).
fn starts_with_word(s: &str, word: &str) -> bool {
    if !s.starts_with(word) {
        return false;
    }
    // Must be followed by non-word char or end of string
    match s.as_bytes().get(word.len()) {
        None => true,
        Some(&b) => !b.is_ascii_alphanumeric() && b != b'_',
    }
}

fn skip_whitespace(s: &str) -> &str {
    s.trim_start()
}

fn starts_with_deql_concept(s: &str) -> bool {
    starts_with_word(s, "AGGREGATE")
        || starts_with_word(s, "COMMAND")
        || starts_with_word(s, "EVENT")
        || starts_with_word(s, "DECISION")
        || starts_with_word(s, "PROJECTION")
        || starts_with_word(s, "EVENTSTORE")
        || starts_with_word(s, "TEMPLATE")
}

/// Strip leading block comments (`/* ... */`) and line comments (`-- ...`)
/// so the classifier sees the first real token.
fn strip_leading_comments(s: &str) -> &str {
    let mut rest = s;
    loop {
        rest = rest.trim_start();
        if rest.starts_with("/*") {
            // Find closing */
            if let Some(end) = rest.find("*/") {
                rest = &rest[end + 2..];
                continue;
            }
            // Unclosed block comment — return as-is
            return rest;
        }
        if rest.starts_with("--") {
            // Skip to end of line
            if let Some(nl) = rest.find('\n') {
                rest = &rest[nl + 1..];
                continue;
            }
            // No newline — entire rest is a comment
            return "";
        }
        return rest;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // DeQL CREATE statements
    #[test]
    fn test_create_aggregate() {
        assert_eq!(
            classify_statement("CREATE AGGREGATE Inventory;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_create_or_replace_aggregate() {
        assert_eq!(
            classify_statement("CREATE OR REPLACE AGGREGATE Inventory;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_create_command() {
        assert_eq!(
            classify_statement("CREATE COMMAND RegisterProduct (sku UUID);"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_create_event() {
        assert_eq!(
            classify_statement("CREATE EVENT ProductRegistered (name STRING);"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_create_decision() {
        assert_eq!(
            classify_statement("CREATE DECISION RegisterNewProduct FOR Inventory ON COMMAND RegisterProduct EMIT AS SELECT EVENT ProductRegistered ();"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_create_projection() {
        assert_eq!(
            classify_statement("CREATE PROJECTION StockLevels AS SELECT * FROM events;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_create_eventstore() {
        assert_eq!(
            classify_statement("CREATE EVENTSTORE inventory_local WITH (durable.type = 'parquet');"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_create_template() {
        assert_eq!(
            classify_statement("CREATE TEMPLATE SoftDeletable (EntityName) AS (CREATE AGGREGATE {{EntityName}};);"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_create_or_replace_command() {
        assert_eq!(
            classify_statement("CREATE OR REPLACE COMMAND Foo (x UUID);"),
            StatementKind::Deql
        );
    }

    // DeQL non-CREATE statements
    #[test]
    fn test_execute() {
        assert_eq!(
            classify_statement("EXECUTE RegisterProduct(sku := 'BOLT-001');"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_inspect_decision() {
        assert_eq!(
            classify_statement("INSPECT DECISION RegisterNewProduct FROM test_data INTO results;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_inspect_projection() {
        assert_eq!(
            classify_statement("INSPECT PROJECTION StockLevels FROM events INTO output;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_describe_aggregate() {
        assert_eq!(
            classify_statement("DESCRIBE AGGREGATE Inventory;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_describe_aggregates() {
        assert_eq!(
            classify_statement("DESCRIBE AGGREGATES;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_use_template_classifies_as_sql() {
        assert_eq!(
            classify_statement("USE TEMPLATE SoftDeletable WITH (EntityName = 'Inventory');"),
            StatementKind::Sql
        );
    }

    #[test]
    fn test_apply_template() {
        assert_eq!(
            classify_statement("APPLY TEMPLATE SoftDeletable WITH (EntityName = 'Inventory');"),
            StatementKind::Deql
        );
    }

    // SQL statements
    #[test]
    fn test_select() {
        assert_eq!(
            classify_statement("SELECT * FROM users;"),
            StatementKind::Sql
        );
    }

    #[test]
    fn test_insert() {
        assert_eq!(
            classify_statement("INSERT INTO users VALUES (1, 'Alice');"),
            StatementKind::Sql
        );
    }

    #[test]
    fn test_create_table() {
        assert_eq!(
            classify_statement("CREATE TABLE users (id INT, name VARCHAR);"),
            StatementKind::Sql
        );
    }

    #[test]
    fn test_create_view() {
        assert_eq!(
            classify_statement("CREATE VIEW active_users AS SELECT * FROM users;"),
            StatementKind::Sql
        );
    }

    #[test]
    fn test_create_index() {
        assert_eq!(
            classify_statement("CREATE INDEX idx_users ON users(id);"),
            StatementKind::Sql
        );
    }

    #[test]
    fn test_show() {
        assert_eq!(
            classify_statement("SHOW TABLES;"),
            StatementKind::Sql
        );
    }

    #[test]
    fn test_drop_table() {
        assert_eq!(
            classify_statement("DROP TABLE users;"),
            StatementKind::Sql
        );
    }

    // Edge cases
    #[test]
    fn test_case_insensitive() {
        assert_eq!(
            classify_statement("create aggregate Foo;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_leading_whitespace() {
        assert_eq!(
            classify_statement("   CREATE AGGREGATE Foo;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_tab_after_create() {
        assert_eq!(
            classify_statement("CREATE\tAGGREGATE Foo;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_newline_after_create() {
        assert_eq!(
            classify_statement("CREATE\nAGGREGATE Foo;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_empty_input() {
        assert_eq!(classify_statement(""), StatementKind::Sql);
    }

    #[test]
    fn test_whitespace_only() {
        assert_eq!(classify_statement("   "), StatementKind::Sql);
    }

    #[test]
    fn test_create_or_replace_table_is_sql() {
        assert_eq!(
            classify_statement("CREATE OR REPLACE TABLE users (id INT);"),
            StatementKind::Sql
        );
    }

    #[test]
    fn test_export_dereg() {
        assert_eq!(
            classify_statement("EXPORT DEREG;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_export_dereg_to_file() {
        assert_eq!(
            classify_statement("EXPORT DEREG TO 'output.deql';"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_validate_dereg() {
        assert_eq!(
            classify_statement("VALIDATE DEREG;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_export_case_insensitive() {
        assert_eq!(
            classify_statement("export dereg;"),
            StatementKind::Deql
        );
    }

    #[test]
    fn test_validate_case_insensitive() {
        assert_eq!(
            classify_statement("validate dereg;"),
            StatementKind::Deql
        );
    }
}
