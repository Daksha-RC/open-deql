// DeQL lexer: source text → token stream.

use crate::parser::error::{Diagnostic, Severity};
use crate::parser::token::{Span, Token, TokenKind};

/// Hand-written scanner for DeQL source text.
pub struct Lexer<'a> {
    source: &'a str,
    pos: usize,
    tokens: Vec<Token>,
    diagnostics: Vec<Diagnostic>,
}

impl<'a> Lexer<'a> {
    pub fn new(source: &'a str) -> Self {
        Self {
            source,
            pos: 0,
            tokens: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    pub fn tokenize(mut self) -> (Vec<Token>, Vec<Diagnostic>) {
        while self.pos < self.source.len() {
            self.scan_token();
        }
        // Append Eof token
        self.tokens.push(Token {
            kind: TokenKind::Eof,
            lexeme: String::new(),
            span: Span {
                start: self.source.len(),
                end: self.source.len(),
            },
        });
        (self.tokens, self.diagnostics)
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn peek(&self) -> Option<char> {
        self.source[self.pos..].chars().next()
    }

    fn peek_next(&self) -> Option<char> {
        let mut chars = self.source[self.pos..].chars();
        chars.next();
        chars.next()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.source[self.pos..].chars().next()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn check(&self, expected: char) -> bool {
        self.peek() == Some(expected)
    }

    fn match_char(&mut self, expected: char) -> bool {
        if self.check(expected) {
            self.advance();
            true
        } else {
            false
        }
    }

    // -----------------------------------------------------------------------
    // Main dispatch
    // -----------------------------------------------------------------------

    fn scan_token(&mut self) {
        let ch = match self.peek() {
            Some(c) => c,
            None => return,
        };

        // Whitespace
        if ch.is_ascii_whitespace() {
            self.advance();
            return;
        }

        // Line comment: --
        if ch == '-' && self.peek_next() == Some('-') {
            self.skip_line_comment();
            return;
        }

        // Block comment: /* ... */
        if ch == '/' && self.peek_next() == Some('*') {
            self.skip_block_comment();
            return;
        }

        // Identifiers / keywords
        if ch.is_ascii_alphabetic() || ch == '_' {
            self.scan_identifier_or_keyword();
            return;
        }

        // Numeric literals
        if ch.is_ascii_digit() {
            self.scan_number();
            return;
        }

        // String literal
        if ch == '\'' {
            self.scan_string_literal();
            return;
        }

        // Double-quoted identifier
        if ch == '"' {
            self.scan_quoted_identifier();
            return;
        }

        // Backtick rejection
        if ch == '`' {
            self.reject_backtick();
            return;
        }

        // Square bracket rejection
        if ch == '[' {
            self.reject_square_bracket();
            return;
        }

        // Template placeholder: {{
        if ch == '{' && self.peek_next() == Some('{') {
            self.scan_template_placeholder();
            return;
        }

        // Colon: := or :identifier (bind param) or unrecognized
        if ch == ':' {
            self.scan_colon();
            return;
        }

        // Operators / punctuation
        self.scan_operator_or_punct();
    }

    // -----------------------------------------------------------------------
    // Identifiers and keywords
    // -----------------------------------------------------------------------

    fn scan_identifier_or_keyword(&mut self) {
        let start = self.pos;
        self.advance(); // consume first char
        while let Some(c) = self.peek() {
            if c.is_ascii_alphanumeric() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }
        let lexeme = &self.source[start..self.pos];
        let kind = keyword_lookup(lexeme).unwrap_or(TokenKind::Identifier);
        self.tokens.push(Token {
            kind,
            lexeme: lexeme.to_string(),
            span: Span {
                start,
                end: self.pos,
            },
        });
    }

    // -----------------------------------------------------------------------
    // Numeric literals
    // -----------------------------------------------------------------------

    fn scan_number(&mut self) {
        let start = self.pos;
        // Consume digits and underscores
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }

        // Check for decimal point followed by digit
        if self.check('.') {
            if let Some(next) = self.peek_next() {
                if next.is_ascii_digit() {
                    self.advance(); // consume '.'
                    while let Some(c) = self.peek() {
                        if c.is_ascii_digit() || c == '_' {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                    let raw = &self.source[start..self.pos];
                    let lexeme = raw.replace('_', "");
                    self.tokens.push(Token {
                        kind: TokenKind::DecimalLiteral,
                        lexeme,
                        span: Span {
                            start,
                            end: self.pos,
                        },
                    });
                    return;
                }
            }
        }

        let raw = &self.source[start..self.pos];
        let lexeme = raw.replace('_', "");
        self.tokens.push(Token {
            kind: TokenKind::IntegerLiteral,
            lexeme,
            span: Span {
                start,
                end: self.pos,
            },
        });
    }

    // -----------------------------------------------------------------------
    // String literals
    // -----------------------------------------------------------------------

    fn scan_string_literal(&mut self) {
        let start = self.pos;
        self.advance(); // consume opening '
        let mut content = String::new();
        loop {
            match self.peek() {
                None => {
                    // Unterminated string
                    self.diagnostics.push(Diagnostic {
                        span: Span {
                            start,
                            end: self.pos,
                        },
                        message: "unterminated string literal".to_string(),
                        severity: Severity::Error,
                    });
                    break;
                }
                Some('\'') => {
                    self.advance(); // consume closing '
                    // Check for escaped quote ''
                    if self.check('\'') {
                        content.push('\'');
                        self.advance();
                    } else {
                        break;
                    }
                }
                Some(c) => {
                    content.push(c);
                    self.advance();
                }
            }
        }
        self.tokens.push(Token {
            kind: TokenKind::StringLiteral,
            lexeme: content,
            span: Span {
                start,
                end: self.pos,
            },
        });
    }

    // -----------------------------------------------------------------------
    // Double-quoted identifiers
    // -----------------------------------------------------------------------

    fn scan_quoted_identifier(&mut self) {
        let start = self.pos;
        self.advance(); // consume opening "
        let content_start = self.pos;
        loop {
            match self.peek() {
                None => {
                    self.diagnostics.push(Diagnostic {
                        span: Span {
                            start,
                            end: self.pos,
                        },
                        message: "unterminated double-quoted identifier".to_string(),
                        severity: Severity::Error,
                    });
                    break;
                }
                Some('"') => {
                    let content_end = self.pos;
                    self.advance(); // consume closing "
                    let lexeme = self.source[content_start..content_end].to_string();
                    self.tokens.push(Token {
                        kind: TokenKind::QuotedIdentifier,
                        lexeme,
                        span: Span {
                            start,
                            end: self.pos,
                        },
                    });
                    return;
                }
                Some(_) => {
                    self.advance();
                }
            }
        }
        // If unterminated, still produce a token with what we have
        let lexeme = self.source[content_start..self.pos].to_string();
        self.tokens.push(Token {
            kind: TokenKind::QuotedIdentifier,
            lexeme,
            span: Span {
                start,
                end: self.pos,
            },
        });
    }

    // -----------------------------------------------------------------------
    // Backtick / square bracket rejection
    // -----------------------------------------------------------------------

    fn reject_backtick(&mut self) {
        let start = self.pos;
        self.advance(); // consume `
        // Skip to closing backtick or end of line
        while let Some(c) = self.peek() {
            if c == '`' {
                self.advance();
                break;
            }
            if c == '\n' {
                break;
            }
            self.advance();
        }
        self.diagnostics.push(Diagnostic {
            span: Span {
                start,
                end: self.pos,
            },
            message: "backtick quoting is not supported; use double quotes for quoted identifiers"
                .to_string(),
            severity: Severity::Error,
        });
    }

    fn reject_square_bracket(&mut self) {
        let start = self.pos;
        self.advance(); // consume [
        // Skip to closing ] or end of line
        while let Some(c) = self.peek() {
            if c == ']' {
                self.advance();
                break;
            }
            if c == '\n' {
                break;
            }
            self.advance();
        }
        self.diagnostics.push(Diagnostic {
            span: Span {
                start,
                end: self.pos,
            },
            message:
                "square-bracket quoting is not supported; use double quotes for quoted identifiers"
                    .to_string(),
            severity: Severity::Error,
        });
    }

    // -----------------------------------------------------------------------
    // Template placeholders
    // -----------------------------------------------------------------------

    fn scan_template_placeholder(&mut self) {
        let start = self.pos;
        self.advance(); // consume first {
        self.advance(); // consume second {

        let name_start = self.pos;
        // Expect identifier
        if let Some(c) = self.peek() {
            if c.is_ascii_alphabetic() || c == '_' {
                self.advance();
                while let Some(c) = self.peek() {
                    if c.is_ascii_alphanumeric() || c == '_' {
                        self.advance();
                    } else {
                        break;
                    }
                }
            }
        }
        let name_end = self.pos;

        // Expect }}
        if self.check('}') && self.peek_next() == Some('}') {
            self.advance(); // consume first }
            self.advance(); // consume second }
            let lexeme = self.source[start..self.pos].to_string();
            self.tokens.push(Token {
                kind: TokenKind::TemplatePlaceholder,
                lexeme,
                span: Span {
                    start,
                    end: self.pos,
                },
            });
        } else {
            // Malformed placeholder — produce diagnostic
            let name = &self.source[name_start..name_end];
            self.diagnostics.push(Diagnostic {
                span: Span {
                    start,
                    end: self.pos,
                },
                message: format!(
                    "malformed template placeholder '{{{{{}}}}}': expected closing '}}}}'",
                    name
                ),
                severity: Severity::Error,
            });
        }
    }

    // -----------------------------------------------------------------------
    // Colon: :=, bind param, or unrecognized
    // -----------------------------------------------------------------------

    fn scan_colon(&mut self) {
        let start = self.pos;
        self.advance(); // consume ':'

        // Check for :=
        if self.check('=') {
            self.advance();
            self.tokens.push(Token {
                kind: TokenKind::ColonEquals,
                lexeme: ":=".to_string(),
                span: Span {
                    start,
                    end: self.pos,
                },
            });
            return;
        }

        // Check for bind parameter :identifier
        if let Some(c) = self.peek() {
            if c.is_ascii_alphabetic() || c == '_' {
                self.advance();
                while let Some(c) = self.peek() {
                    if c.is_ascii_alphanumeric() || c == '_' {
                        self.advance();
                    } else {
                        break;
                    }
                }
                let lexeme = self.source[start..self.pos].to_string();
                self.tokens.push(Token {
                    kind: TokenKind::BindParam,
                    lexeme,
                    span: Span {
                        start,
                        end: self.pos,
                    },
                });
                return;
            }
        }

        // Bare colon — unrecognized
        self.diagnostics.push(Diagnostic {
            span: Span {
                start,
                end: self.pos,
            },
            message: "unexpected character ':'".to_string(),
            severity: Severity::Error,
        });
    }

    // -----------------------------------------------------------------------
    // Operators and punctuation
    // -----------------------------------------------------------------------

    fn scan_operator_or_punct(&mut self) {
        let start = self.pos;
        let ch = match self.advance() {
            Some(c) => c,
            None => return,
        };

        match ch {
            '(' => self.push_token(TokenKind::LParen, "(", start),
            ')' => self.push_token(TokenKind::RParen, ")", start),
            ',' => self.push_token(TokenKind::Comma, ",", start),
            ';' => self.push_token(TokenKind::Semicolon, ";", start),
            '.' => self.push_token(TokenKind::Dot, ".", start),
            '$' => self.push_token(TokenKind::Dollar, "$", start),
            '+' => self.push_token(TokenKind::Plus, "+", start),
            '*' => self.push_token(TokenKind::Star, "*", start),
            '/' => self.push_token(TokenKind::Slash, "/", start),
            '=' => self.push_token(TokenKind::Eq, "=", start),
            '-' => {
                // Already checked for -- comment in scan_token, so this is Minus
                self.push_token(TokenKind::Minus, "-", start);
            }
            '<' => {
                if self.match_char('>') {
                    self.push_token(TokenKind::NotEq, "<>", start);
                } else if self.match_char('=') {
                    self.push_token(TokenKind::LtEq, "<=", start);
                } else {
                    self.push_token(TokenKind::Lt, "<", start);
                }
            }
            '>' => {
                if self.match_char('=') {
                    self.push_token(TokenKind::GtEq, ">=", start);
                } else {
                    self.push_token(TokenKind::Gt, ">", start);
                }
            }
            '!' => {
                if self.match_char('=') {
                    self.push_token(TokenKind::NotEq, "!=", start);
                } else {
                    self.diagnostics.push(Diagnostic {
                        span: Span {
                            start,
                            end: self.pos,
                        },
                        message: format!("unexpected character '{}'", ch),
                        severity: Severity::Error,
                    });
                }
            }
            '|' => {
                if self.match_char('|') {
                    self.push_token(TokenKind::Pipe2, "||", start);
                } else {
                    self.diagnostics.push(Diagnostic {
                        span: Span {
                            start,
                            end: self.pos,
                        },
                        message: format!("unexpected character '{}'", ch),
                        severity: Severity::Error,
                    });
                }
            }
            _ => {
                self.diagnostics.push(Diagnostic {
                    span: Span {
                        start,
                        end: self.pos,
                    },
                    message: format!("unexpected character '{}'", ch),
                    severity: Severity::Error,
                });
            }
        }
    }

    fn push_token(&mut self, kind: TokenKind, lexeme: &str, start: usize) {
        self.tokens.push(Token {
            kind,
            lexeme: lexeme.to_string(),
            span: Span {
                start,
                end: self.pos,
            },
        });
    }

    // -----------------------------------------------------------------------
    // Line comments
    // -----------------------------------------------------------------------

    fn skip_line_comment(&mut self) {
        // Skip --
        self.advance();
        self.advance();
        while let Some(c) = self.peek() {
            if c == '\n' {
                break;
            }
            self.advance();
        }
    }

    fn skip_block_comment(&mut self) {
        // Skip /*
        self.advance();
        self.advance();
        while self.pos < self.source.len() {
            if self.peek() == Some('*') && self.peek_next() == Some('/') {
                self.advance(); // *
                self.advance(); // /
                return;
            }
            self.advance();
        }
        // Unclosed block comment — silently ignore (DataFusion does the same)
    }
}

// ---------------------------------------------------------------------------
// Keyword lookup
// ---------------------------------------------------------------------------

fn keyword_lookup(lexeme: &str) -> Option<TokenKind> {
    let lower = lexeme.to_ascii_lowercase();
    match lower.as_str() {
        "create" => Some(TokenKind::Create),
        "or" => Some(TokenKind::Or),
        "replace" => Some(TokenKind::Replace),
        "aggregate" => Some(TokenKind::Aggregate),
        "command" => Some(TokenKind::Command),
        "event" => Some(TokenKind::Event),
        "decision" => Some(TokenKind::Decision),
        "projection" => Some(TokenKind::Projection),
        "eventstore" => Some(TokenKind::Eventstore),
        "template" => Some(TokenKind::Template),
        "for" => Some(TokenKind::For),
        "on" => Some(TokenKind::On),
        "state" => Some(TokenKind::State),
        "as" => Some(TokenKind::As),
        "emit" => Some(TokenKind::Emit),
        "select" => Some(TokenKind::Select),
        "from" => Some(TokenKind::From),
        "where" => Some(TokenKind::Where),
        "group" => Some(TokenKind::Group),
        "by" => Some(TokenKind::By),
        "order" => Some(TokenKind::Order),
        "desc" => Some(TokenKind::Desc),
        "asc" => Some(TokenKind::Asc),
        "with" => Some(TokenKind::With),
        "execute" => Some(TokenKind::Execute),
        "inspect" => Some(TokenKind::Inspect),
        "inspection" => Some(TokenKind::Inspection),
        "describe" => Some(TokenKind::Describe),
        "apply" => Some(TokenKind::Apply),
        "into" => Some(TokenKind::Into),
        "offset" => Some(TokenKind::Offset),
        "limit" => Some(TokenKind::Limit),
        "in" => Some(TokenKind::In),
        "and" => Some(TokenKind::And),
        "not" => Some(TokenKind::Not),
        "is" => Some(TokenKind::Is),
        "null" => Some(TokenKind::Null),
        "true" => Some(TokenKind::True),
        "false" => Some(TokenKind::False),
        "case" => Some(TokenKind::Case),
        "when" => Some(TokenKind::When),
        "then" => Some(TokenKind::Then),
        "else" => Some(TokenKind::Else),
        "end" => Some(TokenKind::End),
        "join" => Some(TokenKind::Join),
        "left" => Some(TokenKind::Left),
        "right" => Some(TokenKind::Right),
        "inner" => Some(TokenKind::Inner),
        "outer" => Some(TokenKind::Outer),
        "union" => Some(TokenKind::Union),
        "all" => Some(TokenKind::All),
        "branch" => Some(TokenKind::Branch),
        "key" => Some(TokenKind::Key),
        "interval" => Some(TokenKind::Interval),
        "dereg" => Some(TokenKind::DeReg),
        "sensitive" => Some(TokenKind::Sensitive),
        "volatile" => Some(TokenKind::Volatile),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lex(input: &str) -> (Vec<Token>, Vec<Diagnostic>) {
        Lexer::new(input).tokenize()
    }

    fn kinds(tokens: &[Token]) -> Vec<TokenKind> {
        tokens.iter().map(|t| t.kind).collect()
    }

    #[test]
    fn test_empty_input() {
        let (tokens, diags) = lex("");
        assert_eq!(kinds(&tokens), vec![TokenKind::Eof]);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_whitespace_only() {
        let (tokens, diags) = lex("   \t\n\r\n  ");
        assert_eq!(kinds(&tokens), vec![TokenKind::Eof]);
        assert!(diags.is_empty());
    }

    #[test]
    fn test_keywords_case_insensitive() {
        let (tokens, _) = lex("CREATE create Create cReAtE");
        assert_eq!(tokens[0].kind, TokenKind::Create);
        assert_eq!(tokens[0].lexeme, "CREATE");
        assert_eq!(tokens[1].kind, TokenKind::Create);
        assert_eq!(tokens[1].lexeme, "create");
        assert_eq!(tokens[2].kind, TokenKind::Create);
        assert_eq!(tokens[2].lexeme, "Create");
        assert_eq!(tokens[3].kind, TokenKind::Create);
        assert_eq!(tokens[3].lexeme, "cReAtE");
    }

    #[test]
    fn test_dereg_keyword() {
        let (tokens, _) = lex("DeReg dereg DEREG");
        assert_eq!(tokens[0].kind, TokenKind::DeReg);
        assert_eq!(tokens[0].lexeme, "DeReg");
        assert_eq!(tokens[1].kind, TokenKind::DeReg);
        assert_eq!(tokens[2].kind, TokenKind::DeReg);
    }

    #[test]
    fn test_identifier() {
        let (tokens, _) = lex("my_var _foo Bar123");
        assert_eq!(tokens[0].kind, TokenKind::Identifier);
        assert_eq!(tokens[0].lexeme, "my_var");
        assert_eq!(tokens[1].kind, TokenKind::Identifier);
        assert_eq!(tokens[1].lexeme, "_foo");
        assert_eq!(tokens[2].kind, TokenKind::Identifier);
        assert_eq!(tokens[2].lexeme, "Bar123");
    }

    #[test]
    fn test_string_literal() {
        let (tokens, diags) = lex("'hello'");
        assert!(diags.is_empty());
        assert_eq!(tokens[0].kind, TokenKind::StringLiteral);
        assert_eq!(tokens[0].lexeme, "hello");
    }

    #[test]
    fn test_string_literal_escaped_quote() {
        let (tokens, diags) = lex("'it''s'");
        assert!(diags.is_empty());
        assert_eq!(tokens[0].kind, TokenKind::StringLiteral);
        assert_eq!(tokens[0].lexeme, "it's");
    }

    #[test]
    fn test_unterminated_string() {
        let (tokens, diags) = lex("'hello");
        assert_eq!(tokens[0].kind, TokenKind::StringLiteral);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("unterminated"));
    }

    #[test]
    fn test_quoted_identifier() {
        let (tokens, diags) = lex("\"my column\"");
        assert!(diags.is_empty());
        assert_eq!(tokens[0].kind, TokenKind::QuotedIdentifier);
        assert_eq!(tokens[0].lexeme, "my column");
    }

    #[test]
    fn test_integer_literal() {
        let (tokens, _) = lex("42 1_000");
        assert_eq!(tokens[0].kind, TokenKind::IntegerLiteral);
        assert_eq!(tokens[0].lexeme, "42");
        assert_eq!(tokens[1].kind, TokenKind::IntegerLiteral);
        assert_eq!(tokens[1].lexeme, "1000");
    }

    #[test]
    fn test_decimal_literal() {
        let (tokens, _) = lex("3.14 1_000.50");
        assert_eq!(tokens[0].kind, TokenKind::DecimalLiteral);
        assert_eq!(tokens[0].lexeme, "3.14");
        assert_eq!(tokens[1].kind, TokenKind::DecimalLiteral);
        assert_eq!(tokens[1].lexeme, "1000.50");
    }

    #[test]
    fn test_bind_param() {
        let (tokens, diags) = lex(":field_name");
        assert!(diags.is_empty());
        assert_eq!(tokens[0].kind, TokenKind::BindParam);
        assert_eq!(tokens[0].lexeme, ":field_name");
    }

    #[test]
    fn test_template_placeholder() {
        let (tokens, diags) = lex("{{EntityName}}");
        assert!(diags.is_empty());
        assert_eq!(tokens[0].kind, TokenKind::TemplatePlaceholder);
        assert_eq!(tokens[0].lexeme, "{{EntityName}}");
    }

    #[test]
    fn test_colon_equals() {
        let (tokens, diags) = lex(":=");
        assert!(diags.is_empty());
        assert_eq!(tokens[0].kind, TokenKind::ColonEquals);
        assert_eq!(tokens[0].lexeme, ":=");
    }

    #[test]
    fn test_operators() {
        let (tokens, diags) = lex("( ) , ; . $ = <> != < > <= >= + - * / ||");
        assert!(diags.is_empty());
        let k = kinds(&tokens);
        assert_eq!(
            &k[..k.len() - 1], // exclude Eof
            &[
                TokenKind::LParen,
                TokenKind::RParen,
                TokenKind::Comma,
                TokenKind::Semicolon,
                TokenKind::Dot,
                TokenKind::Dollar,
                TokenKind::Eq,
                TokenKind::NotEq,
                TokenKind::NotEq,
                TokenKind::Lt,
                TokenKind::Gt,
                TokenKind::LtEq,
                TokenKind::GtEq,
                TokenKind::Plus,
                TokenKind::Minus,
                TokenKind::Star,
                TokenKind::Slash,
                TokenKind::Pipe2,
            ]
        );
    }

    #[test]
    fn test_line_comment() {
        let (tokens, diags) = lex("CREATE -- this is a comment\nAGGREGATE");
        assert!(diags.is_empty());
        assert_eq!(tokens[0].kind, TokenKind::Create);
        assert_eq!(tokens[1].kind, TokenKind::Aggregate);
    }

    #[test]
    fn test_backtick_rejection() {
        let (_, diags) = lex("`my_id`");
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("backtick"));
    }

    #[test]
    fn test_square_bracket_rejection() {
        let (_, diags) = lex("[my_id]");
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("square-bracket"));
    }

    #[test]
    fn test_unrecognized_character() {
        let (_, diags) = lex("§");
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("unexpected character"));
    }

    #[test]
    fn test_span_validity() {
        let input = "CREATE AGGREGATE Foo;";
        let (tokens, _) = lex(input);
        for token in &tokens {
            assert!(token.span.start <= token.span.end);
            assert!(token.span.end <= input.len());
        }
    }

    #[test]
    fn test_full_statement() {
        let input = "CREATE OR REPLACE AGGREGATE Product;";
        let (tokens, diags) = lex(input);
        assert!(diags.is_empty());
        let k = kinds(&tokens);
        assert_eq!(
            k,
            vec![
                TokenKind::Create,
                TokenKind::Or,
                TokenKind::Replace,
                TokenKind::Aggregate,
                TokenKind::Identifier,
                TokenKind::Semicolon,
                TokenKind::Eof,
            ]
        );
        assert_eq!(tokens[4].lexeme, "Product");
    }

    #[test]
    fn test_bare_colon_unrecognized() {
        let (_, diags) = lex(": ");
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("unexpected character ':'"));
    }

    #[test]
    fn test_number_followed_by_dot_no_digit() {
        // 42. should be integer 42 then dot
        let (tokens, diags) = lex("42.foo");
        assert!(diags.is_empty());
        assert_eq!(tokens[0].kind, TokenKind::IntegerLiteral);
        assert_eq!(tokens[0].lexeme, "42");
        assert_eq!(tokens[1].kind, TokenKind::Dot);
        assert_eq!(tokens[2].kind, TokenKind::Identifier);
    }

    #[test]
    fn test_eof_token_at_end() {
        let (tokens, _) = lex("SELECT");
        assert_eq!(tokens.last().unwrap().kind, TokenKind::Eof);
    }
}
