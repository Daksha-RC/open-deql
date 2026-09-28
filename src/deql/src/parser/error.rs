// Diagnostic and error types for the DeQL parser.

use crate::parser::token::Span;

/// Severity level for a diagnostic message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

/// A diagnostic message with source location and severity.
#[derive(Debug, Clone, PartialEq)]
pub struct Diagnostic {
    pub span: Span,
    pub message: String,
    pub severity: Severity,
}

impl Diagnostic {
    /// Format a human-readable error message with line/column numbers and source context.
    ///
    /// Example output:
    /// ```text
    /// error[3:24]: expected keyword FOR after decision name, found 'EMIT'
    ///   |
    /// 3 | CREATE DECISION MyDec EMIT AS
    ///   |                       ^^^^ expected FOR
    /// ```
    pub fn display(&self, source: &str) -> String {
        let severity_str = match self.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };

        // Compute line number (1-based) and column number (1-based) from byte offset.
        let (line, col) = byte_offset_to_line_col(source, self.span.start);

        // Extract the source line containing the error.
        let source_line = get_source_line(source, self.span.start);

        // Compute the underline caret length (at least 1 character).
        let caret_len = (self.span.end.saturating_sub(self.span.start)).max(1);

        // Build the line number string for alignment.
        let line_str = line.to_string();
        let padding = " ".repeat(line_str.len());

        format!(
            "{severity}[{line}:{col}]: {message}\n\
             {padding}  |\n\
             {line_str} | {source_line}\n\
             {padding}  |{col_padding}{carets} {message}",
            severity = severity_str,
            line = line,
            col = col,
            message = self.message,
            padding = padding,
            line_str = line_str,
            source_line = source_line,
            col_padding = " ".repeat(col),
            carets = "^".repeat(caret_len),
        )
    }
}

/// Compute 1-based line and column numbers from a byte offset.
fn byte_offset_to_line_col(source: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(source.len());
    let mut line = 1;
    let mut line_start = 0;

    for (i, ch) in source.char_indices() {
        if i >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            line_start = i + 1;
        }
    }

    let col = offset - line_start + 1;
    (line, col)
}

/// Extract the source line containing the given byte offset.
fn get_source_line(source: &str, offset: usize) -> &str {
    let offset = offset.min(source.len());

    // Find the start of the line.
    let line_start = source[..offset].rfind('\n').map_or(0, |i| i + 1);

    // Find the end of the line.
    let line_end = source[offset..]
        .find('\n')
        .map_or(source.len(), |i| offset + i);

    &source[line_start..line_end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_severity_debug() {
        assert_eq!(format!("{:?}", Severity::Error), "Error");
        assert_eq!(format!("{:?}", Severity::Warning), "Warning");
    }

    #[test]
    fn test_diagnostic_display_error_single_line() {
        let source = "CREATE AGGREGATE ;";
        let diag = Diagnostic {
            span: Span { start: 17, end: 18 },
            message: "expected identifier".to_string(),
            severity: Severity::Error,
        };
        let output = diag.display(source);
        assert!(output.contains("error[1:18]"));
        assert!(output.contains("expected identifier"));
        assert!(output.contains("CREATE AGGREGATE ;"));
        assert!(output.contains("^"));
    }

    #[test]
    fn test_diagnostic_display_multiline() {
        // "CREATE AGGREGATE Foo;\n" = 22 bytes, so line 2 starts at offset 22
        // "CREATE AGGREGATE " = 17 bytes, so ';' is at offset 22+17 = 39
        let source = "CREATE AGGREGATE Foo;\nCREATE AGGREGATE ;\nCREATE AGGREGATE Bar;";
        let diag = Diagnostic {
            span: Span { start: 39, end: 40 },
            message: "expected identifier".to_string(),
            severity: Severity::Error,
        };
        let output = diag.display(source);
        assert!(output.contains("error[2:18]"));
        assert!(output.contains("CREATE AGGREGATE ;"));
    }

    #[test]
    fn test_diagnostic_display_warning() {
        let source = "CREATE AGGREGATE Foo;";
        let diag = Diagnostic {
            span: Span { start: 0, end: 6 },
            message: "deprecated syntax".to_string(),
            severity: Severity::Warning,
        };
        let output = diag.display(source);
        assert!(output.contains("warning[1:1]"));
        assert!(output.contains("deprecated syntax"));
        assert!(output.contains("^^^^^^"));
    }

    #[test]
    fn test_diagnostic_display_with_carets_spanning_multiple_chars() {
        let source = "CREATE DECISION MyDec EMIT AS";
        let diag = Diagnostic {
            span: Span { start: 22, end: 26 },
            message: "expected FOR".to_string(),
            severity: Severity::Error,
        };
        let output = diag.display(source);
        assert!(output.contains("error[1:23]"));
        assert!(output.contains("^^^^"));
        assert!(output.contains("expected FOR"));
    }

    #[test]
    fn test_byte_offset_to_line_col_first_line() {
        let source = "hello world";
        assert_eq!(byte_offset_to_line_col(source, 0), (1, 1));
        assert_eq!(byte_offset_to_line_col(source, 6), (1, 7));
    }

    #[test]
    fn test_byte_offset_to_line_col_second_line() {
        let source = "line one\nline two";
        assert_eq!(byte_offset_to_line_col(source, 9), (2, 1));
        assert_eq!(byte_offset_to_line_col(source, 14), (2, 6));
    }

    #[test]
    fn test_get_source_line_single() {
        let source = "hello world";
        assert_eq!(get_source_line(source, 3), "hello world");
    }

    #[test]
    fn test_get_source_line_multiline() {
        let source = "first\nsecond\nthird";
        assert_eq!(get_source_line(source, 0), "first");
        assert_eq!(get_source_line(source, 6), "second");
        assert_eq!(get_source_line(source, 13), "third");
    }
}
