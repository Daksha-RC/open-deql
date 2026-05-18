// Recursive-descent parser: token stream → AST.

use std::path::Path;

use crate::parser::ast::*;
use crate::parser::error::{Diagnostic, Severity};
use crate::parser::lexer::Lexer;
use crate::parser::token::{Span, Token, TokenKind};

/// Recursive-descent parser for DeQL source text.
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    diagnostics: Vec<Diagnostic>,
    source: String,
}

impl Parser {
    /// Create a new parser from a token stream and original source text.
    pub fn new(tokens: Vec<Token>, source: String) -> Self {
        Self {
            tokens,
            pos: 0,
            diagnostics: Vec::new(),
            source,
        }
    }

    // -----------------------------------------------------------------------
    // Core helpers
    // -----------------------------------------------------------------------

    /// Returns a reference to the current token (or the last Eof token if past end).
    fn peek(&self) -> &Token {
        if self.pos < self.tokens.len() {
            &self.tokens[self.pos]
        } else {
            // The lexer always appends an Eof token, so last() is safe.
            self.tokens.last().unwrap()
        }
    }

    /// Returns the `TokenKind` of the current token.
    fn peek_kind(&self) -> TokenKind {
        self.peek().kind
    }

    /// Consumes the current token and returns a reference to it.
    fn advance(&mut self) -> &Token {
        let tok = self.pos;
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        &self.tokens[tok.min(self.tokens.len() - 1)]
    }

    /// If the current token matches `kind`, consume and return it.
    /// Otherwise emit a diagnostic and return `Err(())`.
    fn expect(&mut self, kind: TokenKind) -> Result<Token, ()> {
        if self.peek_kind() == kind {
            Ok(self.advance().clone())
        } else {
            self.error(format!(
                "expected {:?}, found '{}'",
                kind,
                self.peek().lexeme
            ));
            Err(())
        }
    }

    /// Expect an `Identifier` or `QuotedIdentifier` token and return its
    /// name wrapped in a `Spanned<String>`.
    fn expect_identifier(&mut self) -> Result<Spanned<String>, ()> {
        match self.peek_kind() {
            TokenKind::Identifier | TokenKind::QuotedIdentifier => {
                let tok = self.advance().clone();
                Ok(Spanned {
                    node: tok.lexeme,
                    span: tok.span,
                })
            }
            _ => {
                self.error(format!(
                    "expected identifier, found '{}'",
                    self.peek().lexeme
                ));
                Err(())
            }
        }
    }

    /// Check whether the current token is `kind` without consuming it.
    fn check(&self, kind: TokenKind) -> bool {
        self.peek_kind() == kind
    }

    /// Returns `true` if the parser has reached the end of input.
    fn at_end(&self) -> bool {
        self.peek_kind() == TokenKind::Eof
    }

    /// Push a diagnostic error at the current token's span.
    fn error(&mut self, message: String) {
        let span = self.peek().span;
        self.diagnostics.push(Diagnostic {
            span,
            message,
            severity: Severity::Error,
        });
    }

    /// Push a diagnostic error at the given span.
    fn error_at(&mut self, span: Span, message: String) {
        self.diagnostics.push(Diagnostic {
            span,
            message,
            severity: Severity::Error,
        });
    }

    // -----------------------------------------------------------------------
    // Error recovery
    // -----------------------------------------------------------------------

    /// Advance to the next semicolon or Eof (statement-level recovery).
    fn synchronize(&mut self) {
        while !self.at_end() {
            if self.peek_kind() == TokenKind::Semicolon {
                self.advance(); // consume the semicolon
                return;
            }
            self.advance();
        }
    }

    /// Advance to the closing `)` respecting nesting (sub-structure recovery).
    fn synchronize_to_paren(&mut self) {
        let mut depth: usize = 1;
        while !self.at_end() {
            match self.peek_kind() {
                TokenKind::LParen => {
                    depth += 1;
                    self.advance();
                }
                TokenKind::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        self.advance(); // consume the closing paren
                        return;
                    }
                    self.advance();
                }
                _ => {
                    self.advance();
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Main parse loop
    // -----------------------------------------------------------------------

    /// Consume the parser and produce a `ParsedSource` plus accumulated diagnostics.
    pub fn parse(mut self) -> (ParsedSource, Vec<Diagnostic>) {
        let mut statements = Vec::new();
        while !self.at_end() {
            // Skip bare semicolons
            if self.check(TokenKind::Semicolon) {
                self.advance();
                continue;
            }
            match self.parse_statement() {
                Ok(stmt) => statements.push(stmt),
                Err(()) => self.synchronize(),
            }
        }
        (ParsedSource { statements }, self.diagnostics)
    }

    fn parse_statement(&mut self) -> Result<Spanned<DeqlStatement>, ()> {
        let start_span = self.peek().span;
        match self.peek_kind() {
            TokenKind::Create => self.parse_create_statement(start_span),
            TokenKind::Execute => self.parse_execute_statement(start_span),
            TokenKind::Inspect => self.parse_inspect_statement(start_span),
            TokenKind::Describe => self.parse_describe_statement(start_span),
            TokenKind::Apply => self.parse_apply_template_statement(start_span),
            TokenKind::Identifier => {
                let lexeme = self.peek().lexeme.to_ascii_lowercase();
                match lexeme.as_str() {
                    "export" => self.parse_export_statement(start_span),
                    "validate" => self.parse_validate_statement(start_span),
                    "use" => {
                        // Check if this is "USE TEMPLATE" — emit helpful error
                        let use_span = self.peek().span;
                        self.advance(); // consume "use" identifier
                        if self.check(TokenKind::Template) {
                            self.error_at(
                                use_span,
                                "USE TEMPLATE is no longer supported; use APPLY TEMPLATE instead"
                                    .to_string(),
                            );
                            return Err(());
                        }
                        self.error_at(
                            use_span,
                            format!("unexpected token 'use', expected a DeQL statement"),
                        );
                        Err(())
                    }
                    _ => {
                        self.error(format!(
                            "unexpected token '{}', expected a DeQL statement",
                            self.peek().lexeme
                        ));
                        Err(())
                    }
                }
            }
            _ => {
                self.error(format!(
                    "unexpected token '{}', expected a DeQL statement",
                    self.peek().lexeme
                ));
                Err(())
            }
        }
    }

    // -----------------------------------------------------------------------
    // CREATE statement dispatcher
    // -----------------------------------------------------------------------

    fn parse_create_statement(&mut self, start_span: Span) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume CREATE

        // Check for OR REPLACE
        let or_replace = if self.check(TokenKind::Or) {
            self.advance(); // consume OR
            self.expect(TokenKind::Replace)?; // expect REPLACE
            true
        } else {
            false
        };

        match self.peek_kind() {
            TokenKind::Aggregate => self.parse_create_aggregate(start_span, or_replace),
            TokenKind::Command => self.parse_create_command(start_span, or_replace),
            TokenKind::Event => self.parse_create_event(start_span, or_replace),
            TokenKind::Decision => self.parse_create_decision(start_span, or_replace),
            TokenKind::Projection => self.parse_create_projection(start_span, or_replace),
            TokenKind::Eventstore => self.parse_create_eventstore(start_span, or_replace),
            TokenKind::Template => self.parse_create_template(start_span, or_replace),
            _ => {
                self.error(format!(
                    "expected AGGREGATE, COMMAND, EVENT, DECISION, PROJECTION, EVENTSTORE, or TEMPLATE after CREATE, found '{}'",
                    self.peek().lexeme
                ));
                Err(())
            }
        }
    }

    // -----------------------------------------------------------------------
    // Shared helpers: type parsing and field list parsing
    // -----------------------------------------------------------------------

    /// Parse a DeQL type: UUID, STRING, INT, DECIMAL(p,s), TIMESTAMP, BOOLEAN.
    /// Type keywords are Identifier tokens — we match on the lowercased lexeme.
    fn parse_deql_type(&mut self) -> Result<Spanned<DeqlType>, ()> {
        if self.peek_kind() != TokenKind::Identifier {
            self.error(format!(
                "expected type (UUID, STRING, INT, DECIMAL, TIMESTAMP, BOOLEAN), found '{}'",
                self.peek().lexeme
            ));
            return Err(());
        }
        let tok = self.advance().clone();
        let span = tok.span;
        match tok.lexeme.to_ascii_lowercase().as_str() {
            "uuid" => Ok(Spanned {
                node: DeqlType::Uuid,
                span,
            }),
            "string" => Ok(Spanned {
                node: DeqlType::String,
                span,
            }),
            "int" => Ok(Spanned {
                node: DeqlType::Int,
                span,
            }),
            "timestamp" => Ok(Spanned {
                node: DeqlType::Timestamp,
                span,
            }),
            "boolean" => Ok(Spanned {
                node: DeqlType::Boolean,
                span,
            }),
            "decimal" => {
                // DECIMAL or DECIMAL(p,s) — parentheses optional, defaults to DECIMAL(38,18)
                if self.check(TokenKind::LParen) {
                    self.advance(); // consume (
                    let p_tok = self.expect(TokenKind::IntegerLiteral)?;
                    let precision: u8 = p_tok.lexeme.parse().unwrap_or(0);
                    self.expect(TokenKind::Comma)?;
                    let s_tok = self.expect(TokenKind::IntegerLiteral)?;
                    let scale: u8 = s_tok.lexeme.parse().unwrap_or(0);
                    let end_tok = self.expect(TokenKind::RParen)?;
                    Ok(Spanned {
                        node: DeqlType::Decimal { precision, scale },
                        span: span.merge(end_tok.span),
                    })
                } else {
                    Ok(Spanned {
                        node: DeqlType::Decimal {
                            precision: 38,
                            scale: 18,
                        },
                        span,
                    })
                }
            }
            _ => {
                self.error_at(
                    span,
                    format!(
                        "unknown type '{}'; expected UUID, STRING, INT, DECIMAL, TIMESTAMP, or BOOLEAN",
                        tok.lexeme
                    ),
                );
                Err(())
            }
        }
    }

    /// Parse a parenthesized field list: `(<name> <TYPE> [KEY], ...)`.
    fn parse_field_list(&mut self) -> Result<Vec<FieldDef>, ()> {
        self.expect(TokenKind::LParen)?;
        let mut fields = Vec::new();
        loop {
            if self.check(TokenKind::RParen) {
                break;
            }
            let name = match self.expect_identifier() {
                Ok(n) => n,
                Err(()) => {
                    self.synchronize_to_paren();
                    return Err(());
                }
            };
            let data_type = match self.parse_deql_type() {
                Ok(t) => t,
                Err(()) => {
                    self.synchronize_to_paren();
                    return Err(());
                }
            };
            let is_key = if self.check(TokenKind::Key) {
                self.advance();
                true
            } else {
                false
            };

            // Parse optional annotation (SENSITIVE or VOLATILE)
            let mut annotation = None;
            if self.check(TokenKind::Sensitive) {
                self.advance();
                annotation = Some(FieldAnnotation::Sensitive);
                // Check for both annotations (error)
                if self.check(TokenKind::Volatile) {
                    self.error(
                        "Only one annotation (SENSITIVE or VOLATILE) allowed per field."
                            .to_string(),
                    );
                    self.advance();
                }
            } else if self.check(TokenKind::Volatile) {
                self.advance();
                annotation = Some(FieldAnnotation::Volatile);
                // Check for both annotations (error)
                if self.check(TokenKind::Sensitive) {
                    self.error(
                        "Only one annotation (SENSITIVE or VOLATILE) allowed per field."
                            .to_string(),
                    );
                    self.advance();
                }
            }

            // Unknown annotation error
            if self.check(TokenKind::Identifier) {
                let tok = self.peek();
                self.error(format!("Unknown annotation: {}", tok.lexeme));
                self.advance();
            }

            fields.push(FieldDef {
                name,
                data_type,
                is_key,
                annotation,
            });
            // Allow comma after annotation
            if self.check(TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        Ok(fields)
    }

    // -----------------------------------------------------------------------
    // CREATE AGGREGATE (task 3.2)
    // -----------------------------------------------------------------------

    fn parse_create_aggregate(
        &mut self,
        start: Span,
        or_replace: bool,
    ) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume AGGREGATE
        let name = self.expect_identifier()?;
        let fields = if self.check(TokenKind::LParen) {
            Some(self.parse_field_list()?)
        } else {
            None
        };
        let end = self.expect(TokenKind::Semicolon)?;
        Ok(Spanned {
            node: DeqlStatement::CreateAggregate(CreateAggregate {
                or_replace,
                name,
                fields,
            }),
            span: start.merge(end.span),
        })
    }

    // -----------------------------------------------------------------------
    // CREATE COMMAND (task 3.3)
    // -----------------------------------------------------------------------

    fn parse_create_command(
        &mut self,
        start: Span,
        or_replace: bool,
    ) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume COMMAND
        let name = self.expect_identifier()?;
        let fields = self.parse_field_list()?;
        let end = self.expect(TokenKind::Semicolon)?;
        Ok(Spanned {
            node: DeqlStatement::CreateCommand(CreateCommand {
                or_replace,
                name,
                fields,
            }),
            span: start.merge(end.span),
        })
    }

    // -----------------------------------------------------------------------
    // CREATE EVENT (task 3.3)
    // -----------------------------------------------------------------------

    fn parse_create_event(
        &mut self,
        start: Span,
        or_replace: bool,
    ) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume EVENT
        let name = self.expect_identifier()?;
        let fields = self.parse_field_list()?;
        let end = self.expect(TokenKind::Semicolon)?;
        Ok(Spanned {
            node: DeqlStatement::CreateEvent(CreateEvent {
                or_replace,
                name,
                fields,
            }),
            span: start.merge(end.span),
        })
    }

    // -----------------------------------------------------------------------
    // SQL fragment capture helpers
    // -----------------------------------------------------------------------

    /// Capture raw source text from current position until one of the stop
    /// token kinds is found at paren depth 0. Tracks paren depth and string
    /// literals to avoid false matches. Returns the captured tokens' lexemes
    /// joined with spaces as a `SqlFragment`.
    fn capture_sql_until(&mut self, stop_kinds: &[TokenKind]) -> SqlFragment {
        let start_span = self.peek().span;
        let mut parts: Vec<String> = Vec::new();
        let mut depth: usize = 0;
        let mut end_span = start_span;

        while !self.at_end() {
            if depth == 0 && stop_kinds.contains(&self.peek_kind()) {
                break;
            }
            match self.peek_kind() {
                TokenKind::LParen => depth += 1,
                TokenKind::RParen => {
                    if depth > 0 {
                        depth -= 1;
                    }
                }
                _ => {}
            }
            let tok = self.advance().clone();
            end_span = tok.span;
            // Reconstruct string literals with quotes
            if tok.kind == TokenKind::StringLiteral {
                parts.push(format!("'{}'", tok.lexeme));
            } else if tok.kind == TokenKind::QuotedIdentifier {
                parts.push(format!("\"{}\"", tok.lexeme));
            } else {
                parts.push(tok.lexeme);
            }
        }

        let sql = parts.join(" ").trim().to_string();
        SqlFragment {
            sql,
            span: start_span.merge(end_span),
        }
    }

    /// Capture STATE AS SQL body. Stops at:
    /// - `EMIT` keyword at depth 0 (start of `EMIT AS` clause)
    /// - `AS` keyword at depth 0 when followed by `SELECT` (start of shorthand emit clause)
    fn capture_state_as_sql(&mut self) -> SqlFragment {
        let start_span = self.peek().span;
        let mut parts: Vec<String> = Vec::new();
        let mut depth: usize = 0;
        let mut end_span = start_span;

        while !self.at_end() {
            if depth == 0 {
                // Stop on EMIT keyword
                if self.peek_kind() == TokenKind::Emit {
                    break;
                }
                // Stop on AS keyword when followed by SELECT (emit clause shorthand)
                if self.peek_kind() == TokenKind::As {
                    let next_pos = self.pos + 1;
                    if next_pos < self.tokens.len()
                        && self.tokens[next_pos].kind == TokenKind::Select
                    {
                        break;
                    }
                }
            }
            match self.peek_kind() {
                TokenKind::LParen => depth += 1,
                TokenKind::RParen => {
                    if depth > 0 {
                        depth -= 1;
                    }
                }
                _ => {}
            }
            let tok = self.advance().clone();
            end_span = tok.span;
            if tok.kind == TokenKind::StringLiteral {
                parts.push(format!("'{}'", tok.lexeme));
            } else if tok.kind == TokenKind::QuotedIdentifier {
                parts.push(format!("\"{}\"", tok.lexeme));
            } else {
                parts.push(tok.lexeme);
            }
        }

        let sql = parts.join(" ").trim().to_string();
        SqlFragment {
            sql,
            span: start_span.merge(end_span),
        }
    }

    /// Capture a dotted/dollar reference like `DeReg.Inventory$Events`
    /// or a simple identifier. Returns the full reference as a string.
    fn parse_dotted_reference(&mut self) -> Result<Spanned<String>, ()> {
        let first = self.expect_identifier_or_keyword()?;
        let mut name = first.node.clone();
        let start_span = first.span;
        let mut end_span = first.span;

        loop {
            if self.check(TokenKind::Dot) {
                self.advance(); // consume dot
                // After dot, accept identifier or keyword tokens
                let part = self.expect_identifier_or_keyword()?;
                end_span = part.span;
                name.push('.');
                name.push_str(&part.node);
            } else if self.check(TokenKind::Dollar) {
                self.advance(); // consume $
                let part = self.expect_identifier_or_keyword()?;
                end_span = part.span;
                name.push('$');
                name.push_str(&part.node);
            } else {
                break;
            }
        }

        Ok(Spanned {
            node: name,
            span: start_span.merge(end_span),
        })
    }

    /// Like `expect_identifier` but also accepts keyword tokens that might
    /// appear in dotted references (e.g., DeReg is a keyword).
    fn expect_identifier_or_keyword(&mut self) -> Result<Spanned<String>, ()> {
        match self.peek_kind() {
            TokenKind::Identifier
            | TokenKind::QuotedIdentifier
            | TokenKind::DeReg
            | TokenKind::Aggregate
            | TokenKind::Command
            | TokenKind::Event
            | TokenKind::Decision
            | TokenKind::Projection
            | TokenKind::Eventstore
            | TokenKind::Template
            | TokenKind::State
            | TokenKind::Key
            | TokenKind::Select
            | TokenKind::From
            | TokenKind::Where
            | TokenKind::Order
            | TokenKind::Group
            | TokenKind::By
            | TokenKind::Join
            | TokenKind::Left
            | TokenKind::Right
            | TokenKind::Inner
            | TokenKind::Outer
            | TokenKind::Union
            | TokenKind::All
            | TokenKind::And
            | TokenKind::Or
            | TokenKind::Not
            | TokenKind::Is
            | TokenKind::Null
            | TokenKind::True
            | TokenKind::False
            | TokenKind::In
            | TokenKind::Case
            | TokenKind::When
            | TokenKind::Then
            | TokenKind::Else
            | TokenKind::End
            | TokenKind::Asc
            | TokenKind::Desc
            | TokenKind::Limit
            | TokenKind::Offset
            | TokenKind::Into
            | TokenKind::Interval
            | TokenKind::TemplatePlaceholder => {
                let tok = self.advance().clone();
                Ok(Spanned {
                    node: tok.lexeme,
                    span: tok.span,
                })
            }
            _ => {
                self.error(format!(
                    "expected identifier, found '{}'",
                    self.peek().lexeme
                ));
                Err(())
            }
        }
    }

    // -----------------------------------------------------------------------
    // CREATE DECISION (task 5.1)
    // -----------------------------------------------------------------------

    fn parse_create_decision(
        &mut self,
        start: Span,
        or_replace: bool,
    ) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume DECISION
        let name = self.expect_identifier()?;

        // FOR <Aggregate>
        self.expect(TokenKind::For)?;
        let aggregate = self.expect_identifier()?;

        // ON COMMAND <Command>
        self.expect(TokenKind::On)?;
        self.expect(TokenKind::Command)?;
        let command = self.expect_identifier()?;

        // Optional STATE AS <SQL>
        let state_as = if self.check(TokenKind::State) {
            self.advance(); // consume STATE
            self.expect(TokenKind::As)?;
            let frag = self.capture_state_as_sql();
            if frag.sql.is_empty() {
                None
            } else {
                Some(frag)
            }
        } else {
            None
        };

        // EMIT AS or AS
        if self.check(TokenKind::Emit) {
            self.advance(); // consume EMIT
            self.expect(TokenKind::As)?;
        } else if self.check(TokenKind::As) {
            self.advance(); // consume AS (shorthand)
        } else {
            self.error(format!(
                "expected EMIT AS or AS, found '{}'",
                self.peek().lexeme
            ));
            return Err(());
        }

        // Parse branches separated by UNION ALL.
        // Each branch may start with an optional BRANCH <RuleName> label,
        // contains comma-separated emit items, and an optional WHERE guard.
        let mut branches = Vec::new();
        let mut branch_index: usize = 1;
        loop {
            let branch = self.parse_decision_branch(branch_index)?;
            branches.push(branch);
            branch_index += 1;

            // Check for UNION ALL separator
            if self.check(TokenKind::Union) {
                self.advance(); // consume UNION
                self.expect(TokenKind::All)?;
            } else {
                break;
            }
        }

        let end = self.expect(TokenKind::Semicolon)?;
        Ok(Spanned {
            node: DeqlStatement::CreateDecision(CreateDecision {
                or_replace,
                name,
                aggregate,
                command,
                state_as,
                branches,
            }),
            span: start.merge(end.span),
        })
    }

    /// Parse a single decision branch:
    /// `[BRANCH <RuleName>] <emit_items> [WHERE <guard>]`
    fn parse_decision_branch(&mut self, branch_index: usize) -> Result<DecisionBranch, ()> {
        let branch_start = self.peek().span;

        // Optional BRANCH <RuleName> label
        let rule_name = if self.check(TokenKind::Branch) {
            self.advance(); // consume BRANCH
            Some(self.expect_identifier()?)
        } else {
            None
        };

        // Parse comma-separated emit items for this branch
        let mut emit_items = Vec::new();
        loop {
            let item = self.parse_emit_item()?;
            emit_items.push(item);
            // Comma separates emit items within a branch, but only if
            // the next token after comma is SELECT (not UNION or ;).
            if self.check(TokenKind::Comma) {
                // Peek past the comma to see if another emit item follows.
                // If next is SELECT, consume comma and continue.
                // Otherwise the comma is not ours (shouldn't happen in
                // well-formed input, but be safe).
                let next_idx = self.pos + 1;
                if next_idx < self.tokens.len() && self.tokens[next_idx].kind == TokenKind::Select {
                    self.advance(); // consume comma
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        // Optional WHERE guard for this branch
        // A WHERE is consumed here only if we are NOT at a UNION ALL or ; boundary
        // that would indicate the next branch or end of decision.
        let guard = if self.check(TokenKind::Where) {
            self.advance(); // consume WHERE
            let frag = self.capture_sql_until(&[TokenKind::Semicolon, TokenKind::Union]);
            if frag.sql.is_empty() {
                None
            } else {
                Some(frag)
            }
        } else {
            None
        };

        let branch_end = if let Some(ref g) = guard {
            g.span
        } else if let Some(last) = emit_items.last() {
            last.span
        } else {
            branch_start
        };

        Ok(DecisionBranch {
            branch_index,
            rule_name,
            guard,
            emit_items,
            span: branch_start.merge(branch_end),
        })
    }

    /// Parse a single emit item: `SELECT EVENT <Name> (<field> := <expr>, ...)`
    fn parse_emit_item(&mut self) -> Result<EmitItem, ()> {
        let start_span = self.peek().span;
        self.expect(TokenKind::Select)?;
        self.expect(TokenKind::Event)?;
        let event_type = self.expect_identifier_or_keyword()?;
        self.expect(TokenKind::LParen)?;

        let mut assignments = Vec::new();
        loop {
            if self.check(TokenKind::RParen) {
                break;
            }
            let field = self.expect_identifier_or_keyword()?;
            self.expect(TokenKind::ColonEquals)?;
            let value = self.capture_emit_expression()?;
            assignments.push(Assignment { field, value });
            if self.check(TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        let end_tok = self.expect(TokenKind::RParen)?;

        Ok(EmitItem {
            event_type,
            assignments,
            span: start_span.merge(end_tok.span),
        })
    }

    /// Capture an expression inside an emit item assignment.
    /// Captures tokens until `,` or `)` at the current paren depth (0).
    fn capture_emit_expression(&mut self) -> Result<Spanned<String>, ()> {
        let start_span = self.peek().span;
        let mut parts: Vec<String> = Vec::new();
        let mut depth: usize = 0;
        let mut end_span = start_span;

        while !self.at_end() {
            match self.peek_kind() {
                TokenKind::Comma | TokenKind::RParen if depth == 0 => break,
                TokenKind::LParen => {
                    depth += 1;
                    let tok = self.advance().clone();
                    end_span = tok.span;
                    parts.push(tok.lexeme);
                }
                TokenKind::RParen => {
                    depth -= 1;
                    let tok = self.advance().clone();
                    end_span = tok.span;
                    parts.push(tok.lexeme);
                }
                _ => {
                    let tok = self.advance().clone();
                    end_span = tok.span;
                    if tok.kind == TokenKind::StringLiteral {
                        parts.push(format!("'{}'", tok.lexeme));
                    } else {
                        parts.push(tok.lexeme);
                    }
                }
            }
        }

        let expr = parts.join(" ").trim().to_string();
        if expr.is_empty() {
            self.error("expected expression after :=".to_string());
            return Err(());
        }
        Ok(Spanned {
            node: expr,
            span: start_span.merge(end_span),
        })
    }

    // -----------------------------------------------------------------------
    // CREATE PROJECTION (task 5.2)
    // -----------------------------------------------------------------------

    fn parse_create_projection(
        &mut self,
        start: Span,
        or_replace: bool,
    ) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume PROJECTION
        let name = self.expect_identifier()?;
        self.expect(TokenKind::As)?;
        let body = self.capture_sql_until(&[TokenKind::Semicolon]);
        let end = self.expect(TokenKind::Semicolon)?;
        Ok(Spanned {
            node: DeqlStatement::CreateProjection(CreateProjection {
                or_replace,
                name,
                body,
            }),
            span: start.merge(end.span),
        })
    }

    // -----------------------------------------------------------------------
    // CREATE EVENTSTORE (task 5.3)
    // -----------------------------------------------------------------------

    fn parse_create_eventstore(
        &mut self,
        start: Span,
        or_replace: bool,
    ) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume EVENTSTORE
        let name = self.expect_identifier()?;

        // WITH (...) is optional — defaults to empty config
        let config = if self.check(TokenKind::With) {
            self.advance(); // consume WITH
            self.expect(TokenKind::LParen)?;

            let mut pairs = Vec::new();
            loop {
                if self.check(TokenKind::RParen) {
                    break;
                }
                let pair = self.parse_config_pair()?;
                pairs.push(pair);
                if self.check(TokenKind::Comma) {
                    self.advance();
                }
            }
            self.expect(TokenKind::RParen)?;
            pairs
        } else {
            Vec::new()
        };

        let end = self.expect(TokenKind::Semicolon)?;

        Ok(Spanned {
            node: DeqlStatement::CreateEventStore(CreateEventStore {
                or_replace,
                name,
                config,
            }),
            span: start.merge(end.span),
        })
    }

    /// Parse a single config pair: `dotted.key = value`
    fn parse_config_pair(&mut self) -> Result<ConfigPair, ()> {
        // Parse dotted key
        let first = self.expect_identifier_or_keyword()?;
        let mut key = first.node.clone();
        let key_start = first.span;
        let mut key_end = first.span;

        while self.check(TokenKind::Dot) {
            self.advance(); // consume dot
            let part = self.expect_identifier_or_keyword()?;
            key_end = part.span;
            key.push('.');
            key.push_str(&part.node);
        }

        self.expect(TokenKind::Eq)?;

        // Parse value
        let value = self.parse_config_value()?;

        Ok(ConfigPair {
            key: Spanned {
                node: key,
                span: key_start.merge(key_end),
            },
            value,
        })
    }

    /// Parse a config value: string, integer, decimal, boolean, or parenthesized list.
    fn parse_config_value(&mut self) -> Result<Spanned<ConfigValue>, ()> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::StringLiteral => {
                self.advance();
                Ok(Spanned {
                    node: ConfigValue::StringLit(tok.lexeme),
                    span: tok.span,
                })
            }
            TokenKind::IntegerLiteral => {
                self.advance();
                let val: i64 = tok.lexeme.replace('_', "").parse().unwrap_or(0);
                Ok(Spanned {
                    node: ConfigValue::IntLit(val),
                    span: tok.span,
                })
            }
            TokenKind::DecimalLiteral => {
                self.advance();
                let val: f64 = tok.lexeme.replace('_', "").parse().unwrap_or(0.0);
                Ok(Spanned {
                    node: ConfigValue::DecimalLit(val),
                    span: tok.span,
                })
            }
            TokenKind::True => {
                self.advance();
                Ok(Spanned {
                    node: ConfigValue::BoolLit(true),
                    span: tok.span,
                })
            }
            TokenKind::False => {
                self.advance();
                Ok(Spanned {
                    node: ConfigValue::BoolLit(false),
                    span: tok.span,
                })
            }
            TokenKind::LParen => {
                // Parenthesized list of strings: ('a', 'b', ...)
                let start_span = tok.span;
                self.advance(); // consume (
                let mut items = Vec::new();
                loop {
                    if self.check(TokenKind::RParen) {
                        break;
                    }
                    let s = self.expect(TokenKind::StringLiteral)?;
                    items.push(s.lexeme);
                    if self.check(TokenKind::Comma) {
                        self.advance();
                    } else {
                        break;
                    }
                }
                let end_tok = self.expect(TokenKind::RParen)?;
                Ok(Spanned {
                    node: ConfigValue::List(items),
                    span: start_span.merge(end_tok.span),
                })
            }
            _ => {
                self.error(format!(
                    "expected config value (string, integer, decimal, boolean, or list), found '{}'",
                    tok.lexeme
                ));
                Err(())
            }
        }
    }

    // -----------------------------------------------------------------------
    // CREATE TEMPLATE (task 5.4)
    // -----------------------------------------------------------------------

    fn parse_create_template(
        &mut self,
        start: Span,
        or_replace: bool,
    ) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume TEMPLATE
        let name = self.expect_identifier()?;

        // Parse parameters — two variants:
        // 1. (Param, ...) — untyped
        // 2. WITH (Param TYPE, ...) — typed
        let params = if self.check(TokenKind::With) {
            self.advance(); // consume WITH
            self.parse_template_params(true)?
        } else if self.check(TokenKind::LParen) {
            self.parse_template_params(false)?
        } else {
            self.error(format!(
                "expected '(' or WITH after template name, found '{}'",
                self.peek().lexeme
            ));
            return Err(());
        };

        if params.is_empty() {
            self.error("template parameter list must not be empty".to_string());
            return Err(());
        }

        self.expect(TokenKind::As)?;

        // Parse body: capture raw text
        // If next is '(', body is delimited by parens; otherwise until ';'
        let (raw_body, end_span) = if self.check(TokenKind::LParen) {
            self.advance(); // consume opening (
            let body_text = self.capture_template_body_paren();
            // After capture, we should be past the closing )
            let end = self.expect(TokenKind::Semicolon)?;
            (body_text, end.span)
        } else {
            // Body until semicolon — capture raw text
            let body_text = self.capture_template_body_semi();
            let end_span = if self.check(TokenKind::Semicolon) {
                self.advance().span
            } else {
                self.peek().span
            };
            (body_text, end_span)
        };

        Ok(Spanned {
            node: DeqlStatement::CreateTemplate(CreateTemplate {
                or_replace,
                name,
                params,
                body: Vec::new(),
                raw_body: Some(raw_body),
            }),
            span: start.merge(end_span),
        })
    }

    /// Parse template parameter list: `(<name> [TYPE], ...)`
    fn parse_template_params(&mut self, typed: bool) -> Result<Vec<TemplateParam>, ()> {
        self.expect(TokenKind::LParen)?;
        let mut params = Vec::new();
        loop {
            if self.check(TokenKind::RParen) {
                break;
            }
            let param_name = self.expect_identifier_or_keyword()?;
            let data_type = if typed {
                Some(self.parse_deql_type()?)
            } else {
                None
            };
            params.push(TemplateParam {
                name: param_name,
                data_type,
            });
            if self.check(TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        Ok(params)
    }

    /// Capture template body delimited by parens. Tracks paren depth.
    /// Returns the raw text between the opening and closing parens,
    /// extracted directly from the original source using span byte offsets.
    fn capture_template_body_paren(&mut self) -> String {
        let mut depth: usize = 1; // we already consumed the opening paren

        // Record the start offset: byte position of the first token inside the body
        let start_offset = self.peek().span.start;

        while !self.at_end() {
            match self.peek_kind() {
                TokenKind::LParen => {
                    depth += 1;
                    self.advance();
                }
                TokenKind::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        // End offset is the start of the closing ')' token
                        let end_offset = self.peek().span.start;
                        self.advance(); // consume closing paren
                        return self.source[start_offset..end_offset].trim().to_string();
                    }
                    self.advance();
                }
                _ => {
                    self.advance();
                }
            }
        }

        // If we hit EOF without finding the closing paren, extract what we have
        let end_offset = self.peek().span.start;
        self.source[start_offset..end_offset].trim().to_string()
    }

    /// Capture template body until a semicolon at depth 0.
    /// Used for the non-parenthesized body variant.
    /// Extracts raw text from the original source using span byte offsets.
    fn capture_template_body_semi(&mut self) -> String {
        let mut depth: usize = 0;

        // Record the start offset: byte position of the first token in the body
        let start_offset = self.peek().span.start;

        while !self.at_end() {
            match self.peek_kind() {
                TokenKind::Semicolon if depth == 0 => {
                    // Don't consume — let caller handle it
                    // Heuristic: if next token after ; is EOF or a top-level keyword, stop.
                    // Otherwise, consume the ; as part of the body.
                    if self.pos + 1 < self.tokens.len() {
                        let next_kind = self.tokens[self.pos + 1].kind;
                        match next_kind {
                            TokenKind::Eof
                            | TokenKind::Create
                            | TokenKind::Execute
                            | TokenKind::Inspect
                            | TokenKind::Describe
                            | TokenKind::Apply => break,
                            _ => {
                                self.advance();
                            }
                        }
                    } else {
                        break;
                    }
                }
                TokenKind::LParen => {
                    depth += 1;
                    self.advance();
                }
                TokenKind::RParen => {
                    if depth > 0 {
                        depth -= 1;
                    }
                    self.advance();
                }
                _ => {
                    self.advance();
                }
            }
        }

        // End offset is the start of the terminating token (semicolon or EOF)
        let end_offset = self.peek().span.start;
        self.source[start_offset..end_offset].trim().to_string()
    }

    // -----------------------------------------------------------------------
    // EXECUTE (task 3.4)
    // -----------------------------------------------------------------------

    fn parse_execute_statement(&mut self, start: Span) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume EXECUTE
        let command = self.expect_identifier()?;
        self.expect(TokenKind::LParen)?;
        let mut assignments = Vec::new();
        loop {
            if self.check(TokenKind::RParen) {
                break;
            }
            let field = self.expect_identifier()?;
            self.expect(TokenKind::ColonEquals)?;
            let value = self.parse_value_expression()?;
            assignments.push(Assignment { field, value });
            if self.check(TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        let end = self.expect(TokenKind::Semicolon)?;
        Ok(Spanned {
            node: DeqlStatement::Execute(Execute {
                command,
                assignments,
            }),
            span: start.merge(end.span),
        })
    }

    /// Parse a simple value expression for EXECUTE assignments.
    /// Handles string literals, integer/decimal literals, booleans, and identifiers.
    fn parse_value_expression(&mut self) -> Result<Spanned<String>, ()> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::StringLiteral => {
                self.advance();
                Ok(Spanned {
                    node: format!("'{}'", tok.lexeme),
                    span: tok.span,
                })
            }
            TokenKind::IntegerLiteral | TokenKind::DecimalLiteral => {
                self.advance();
                Ok(Spanned {
                    node: tok.lexeme,
                    span: tok.span,
                })
            }
            TokenKind::True | TokenKind::False => {
                self.advance();
                Ok(Spanned {
                    node: tok.lexeme,
                    span: tok.span,
                })
            }
            TokenKind::Identifier | TokenKind::QuotedIdentifier => {
                self.advance();
                Ok(Spanned {
                    node: tok.lexeme,
                    span: tok.span,
                })
            }
            _ => {
                self.error(format!("expected value expression, found '{}'", tok.lexeme));
                Err(())
            }
        }
    }

    // -----------------------------------------------------------------------
    // INSPECT (task 5.5)
    // -----------------------------------------------------------------------

    fn parse_inspect_statement(&mut self, start: Span) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume INSPECT

        let is_decision = match self.peek_kind() {
            TokenKind::Decision => {
                self.advance();
                true
            }
            TokenKind::Projection => {
                self.advance();
                false
            }
            _ => {
                self.error(format!(
                    "expected DECISION or PROJECTION after INSPECT, found '{}'",
                    self.peek().lexeme
                ));
                return Err(());
            }
        };

        let name = self.expect_identifier()?;

        // FROM <source>
        self.expect(TokenKind::From)?;
        let from = self.parse_dotted_reference()?;

        // INTO <target>
        self.expect(TokenKind::Into)?;
        let into = self.parse_dotted_reference()?;

        // Optional OFFSET
        let offset = if self.check(TokenKind::Offset) {
            self.advance();
            let tok = self.expect(TokenKind::IntegerLiteral)?;
            let val: i64 = tok.lexeme.replace('_', "").parse().unwrap_or(0);
            Some(Spanned {
                node: val,
                span: tok.span,
            })
        } else {
            None
        };

        // Optional WHERE guard
        let guard = if self.check(TokenKind::Where) {
            self.advance();
            let frag = self.capture_sql_until(&[TokenKind::Limit, TokenKind::Semicolon]);
            if frag.sql.is_empty() {
                None
            } else {
                Some(frag)
            }
        } else {
            None
        };

        // Optional LIMIT
        let limit = if self.check(TokenKind::Limit) {
            self.advance();
            let tok = self.expect(TokenKind::IntegerLiteral)?;
            let val: i64 = tok.lexeme.replace('_', "").parse().unwrap_or(0);
            Some(Spanned {
                node: val,
                span: tok.span,
            })
        } else {
            None
        };

        let end = self.expect(TokenKind::Semicolon)?;

        if is_decision {
            Ok(Spanned {
                node: DeqlStatement::InspectDecision(InspectDecision {
                    name,
                    from,
                    into,
                    offset,
                    guard,
                    limit,
                }),
                span: start.merge(end.span),
            })
        } else {
            Ok(Spanned {
                node: DeqlStatement::InspectProjection(InspectProjection {
                    name,
                    from,
                    into,
                    offset,
                    guard,
                    limit,
                }),
                span: start.merge(end.span),
            })
        }
    }

    // -----------------------------------------------------------------------
    // DESCRIBE (task 3.5)
    // -----------------------------------------------------------------------

    fn parse_describe_statement(&mut self, start: Span) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume DESCRIBE

        // Check for plural concept types (these are Identifier tokens)
        if self.peek_kind() == TokenKind::Identifier {
            let tok = self.peek().clone();
            let lower = tok.lexeme.to_ascii_lowercase();
            let plural = match lower.as_str() {
                "aggregates" => Some(PluralConceptType::Aggregates),
                "commands" => Some(PluralConceptType::Commands),
                "events" => Some(PluralConceptType::Events),
                "decisions" => Some(PluralConceptType::Decisions),
                "projections" => Some(PluralConceptType::Projections),
                "inspections" => Some(PluralConceptType::Inspections),
                "eventstores" => Some(PluralConceptType::EventStores),
                "templates" => Some(PluralConceptType::Templates),
                _ => None,
            };
            if let Some(concept) = plural {
                self.advance(); // consume the plural keyword
                let end = self.expect(TokenKind::Semicolon)?;
                return Ok(Spanned {
                    node: DeqlStatement::Describe(Describe {
                        target: DescribeTarget::ListAll {
                            concept: Spanned {
                                node: concept,
                                span: tok.span,
                            },
                        },
                    }),
                    span: start.merge(end.span),
                });
            }
        }

        // Check for singular concept types (these are keyword tokens)
        let concept_type = match self.peek_kind() {
            TokenKind::Aggregate => Some(ConceptType::Aggregate),
            TokenKind::Command => Some(ConceptType::Command),
            TokenKind::Event => Some(ConceptType::Event),
            TokenKind::Decision => Some(ConceptType::Decision),
            TokenKind::Projection => Some(ConceptType::Projection),
            TokenKind::Inspection => Some(ConceptType::Inspection),
            TokenKind::Eventstore => Some(ConceptType::EventStore),
            TokenKind::Template => Some(ConceptType::Template),
            _ => None,
        };

        if let Some(concept) = concept_type {
            let concept_tok = self.advance().clone();
            let name = self.expect_identifier()?;
            let end = self.expect(TokenKind::Semicolon)?;
            return Ok(Spanned {
                node: DeqlStatement::Describe(Describe {
                    target: DescribeTarget::Single {
                        concept: Spanned {
                            node: concept,
                            span: concept_tok.span,
                        },
                        name,
                    },
                }),
                span: start.merge(end.span),
            });
        }

        self.error(format!(
            "expected concept type after DESCRIBE (AGGREGATE, COMMAND, EVENT, DECISION, PROJECTION, EVENTSTORE, TEMPLATE, or plural form), found '{}'",
            self.peek().lexeme
        ));
        Err(())
    }

    // -----------------------------------------------------------------------
    // APPLY TEMPLATE (task 5.6)
    // -----------------------------------------------------------------------

    fn parse_apply_template_statement(
        &mut self,
        start: Span,
    ) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume APPLY
        self.expect(TokenKind::Template)?;
        let name = self.expect_identifier()?;
        self.expect(TokenKind::With)?;
        self.expect(TokenKind::LParen)?;

        let mut params = Vec::new();
        loop {
            if self.check(TokenKind::RParen) {
                break;
            }
            let param_name = self.expect_identifier()?;
            self.expect(TokenKind::Eq)?;
            let value = self.parse_template_arg_value()?;
            params.push(TemplateArg {
                name: param_name,
                value,
            });
            if self.check(TokenKind::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        self.expect(TokenKind::RParen)?;
        let end = self.expect(TokenKind::Semicolon)?;

        Ok(Spanned {
            node: DeqlStatement::ApplyTemplate(ApplyTemplate { name, params }),
            span: start.merge(end.span),
        })
    }

    /// Parse a template argument value: string, integer, decimal, boolean,
    /// or parenthesized field list.
    fn parse_template_arg_value(&mut self) -> Result<Spanned<TemplateArgValue>, ()> {
        let tok = self.peek().clone();
        match tok.kind {
            TokenKind::StringLiteral => {
                self.advance();
                Ok(Spanned {
                    node: TemplateArgValue::StringLit(tok.lexeme),
                    span: tok.span,
                })
            }
            TokenKind::IntegerLiteral => {
                self.advance();
                let val: i64 = tok.lexeme.replace('_', "").parse().unwrap_or(0);
                Ok(Spanned {
                    node: TemplateArgValue::IntLit(val),
                    span: tok.span,
                })
            }
            TokenKind::DecimalLiteral => {
                self.advance();
                let val: f64 = tok.lexeme.replace('_', "").parse().unwrap_or(0.0);
                Ok(Spanned {
                    node: TemplateArgValue::DecimalLit(val),
                    span: tok.span,
                })
            }
            TokenKind::True => {
                self.advance();
                Ok(Spanned {
                    node: TemplateArgValue::BoolLit(true),
                    span: tok.span,
                })
            }
            TokenKind::False => {
                self.advance();
                Ok(Spanned {
                    node: TemplateArgValue::BoolLit(false),
                    span: tok.span,
                })
            }
            TokenKind::LParen => {
                // Parenthesized field list: (name TYPE, ...)
                let fields = self.parse_field_list()?;
                let span = if let Some(last) = fields.last() {
                    tok.span.merge(last.data_type.span)
                } else {
                    tok.span
                };
                Ok(Spanned {
                    node: TemplateArgValue::FieldList(fields),
                    span,
                })
            }
            _ => {
                self.error(format!(
                    "expected template argument value (string, integer, decimal, boolean, or field list), found '{}'",
                    tok.lexeme
                ));
                Err(())
            }
        }
    }

    // -----------------------------------------------------------------------
    // EXPORT DEREG and VALIDATE DEREG
    // -----------------------------------------------------------------------

    fn parse_export_statement(&mut self, start: Span) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume EXPORT (identifier)

        // Check for METADATA (identifier, case-insensitive)
        if self.peek_kind() == TokenKind::Identifier
            && self.peek().lexeme.to_ascii_lowercase() == "metadata"
        {
            self.advance(); // consume METADATA

            // Optional TO '<path>'
            let path = if self.peek_kind() == TokenKind::Identifier
                && self.peek().lexeme.to_ascii_lowercase() == "to"
            {
                self.advance(); // consume TO
                let path_tok = self.expect(TokenKind::StringLiteral)?;
                Some(Spanned {
                    node: path_tok.lexeme,
                    span: path_tok.span,
                })
            } else {
                None
            };

            let end = self.expect(TokenKind::Semicolon)?;
            return Ok(Spanned {
                node: DeqlStatement::ExportMetadata(ExportMetadata { path }),
                span: start.merge(end.span),
            });
        }

        self.expect(TokenKind::DeReg)?;

        // Optional TO '<path>'
        let path = if self.peek_kind() == TokenKind::Identifier
            && self.peek().lexeme.to_ascii_lowercase() == "to"
        {
            self.advance(); // consume TO
            let path_tok = self.expect(TokenKind::StringLiteral)?;
            Some(Spanned {
                node: path_tok.lexeme,
                span: path_tok.span,
            })
        } else {
            None
        };

        let end = self.expect(TokenKind::Semicolon)?;
        Ok(Spanned {
            node: DeqlStatement::ExportDeReg(ExportDeReg { path }),
            span: start.merge(end.span),
        })
    }

    fn parse_validate_statement(&mut self, start: Span) -> Result<Spanned<DeqlStatement>, ()> {
        self.advance(); // consume VALIDATE (identifier)
        self.expect(TokenKind::DeReg)?;
        let end = self.expect(TokenKind::Semicolon)?;
        Ok(Spanned {
            node: DeqlStatement::ValidateDeReg,
            span: start.merge(end.span),
        })
    }
}

// ---------------------------------------------------------------------------
// Convenience functions
// ---------------------------------------------------------------------------

/// Parse DeQL source text into an AST.
pub fn parse(source: &str) -> (ParsedSource, Vec<Diagnostic>) {
    let (tokens, mut lex_diags) = Lexer::new(source).tokenize();
    let (parsed, mut parse_diags) = Parser::new(tokens, source.to_string()).parse();
    lex_diags.append(&mut parse_diags);
    (parsed, lex_diags)
}

/// Parse a DeQL file into an AST.
pub fn parse_file(path: &Path) -> Result<(ParsedSource, Vec<Diagnostic>), std::io::Error> {
    let source = std::fs::read_to_string(path)?;
    Ok(parse(&source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_input() {
        let (parsed, diags) = parse("");
        assert!(diags.is_empty());
        assert!(parsed.statements.is_empty());
    }

    #[test]
    fn test_bare_semicolons_skipped() {
        let (parsed, diags) = parse(";;;");
        assert!(diags.is_empty());
        assert!(parsed.statements.is_empty());
    }

    #[test]
    fn test_unexpected_token_produces_diagnostic() {
        let (parsed, diags) = parse("FOOBAR;");
        assert!(parsed.statements.is_empty());
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("unexpected token"));
    }

    #[test]
    fn test_create_aggregate_simple() {
        let (parsed, diags) = parse("CREATE AGGREGATE Foo;");
        assert!(diags.is_empty());
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::CreateAggregate(agg) => {
                assert_eq!(agg.name.node, "Foo");
                assert!(!agg.or_replace);
                assert!(agg.fields.is_none());
            }
            _ => panic!("expected CreateAggregate"),
        }
    }

    #[test]
    fn test_statement_spans_capture_each_statement_body() {
        let input = "-- leading comment\nCREATE AGGREGATE Foo;\n/* between */\nCREATE EVENT Bar (id UUID);\n";
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 2);

        let first = &parsed.statements[0].span;
        let second = &parsed.statements[1].span;

        assert_eq!(&input[first.start..first.end], "CREATE AGGREGATE Foo;");
        assert_eq!(&input[second.start..second.end], "CREATE EVENT Bar (id UUID);");
    }

    #[test]
    fn test_create_or_replace_command() {
        let (parsed, diags) = parse("CREATE OR REPLACE COMMAND Bar (id UUID, name STRING);");
        assert!(diags.is_empty());
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::CreateCommand(cmd) => {
                assert_eq!(cmd.name.node, "Bar");
                assert!(cmd.or_replace);
                assert_eq!(cmd.fields.len(), 2);
                assert_eq!(cmd.fields[0].name.node, "id");
                assert_eq!(cmd.fields[0].data_type.node, DeqlType::Uuid);
                assert_eq!(cmd.fields[1].name.node, "name");
                assert_eq!(cmd.fields[1].data_type.node, DeqlType::String);
            }
            _ => panic!("expected CreateCommand"),
        }
    }

    #[test]
    fn test_create_unknown_concept_error() {
        let (parsed, diags) = parse("CREATE TABLE Foo;");
        assert!(parsed.statements.is_empty());
        assert!(!diags.is_empty());
        assert!(
            diags[0]
                .message
                .contains("expected AGGREGATE, COMMAND, EVENT")
        );
    }

    #[test]
    fn test_execute_parses() {
        let (parsed, diags) = parse("EXECUTE DoSomething(x := 42);");
        assert!(diags.is_empty());
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::Execute(exec) => {
                assert_eq!(exec.command.node, "DoSomething");
                assert_eq!(exec.assignments.len(), 1);
                assert_eq!(exec.assignments[0].field.node, "x");
                assert_eq!(exec.assignments[0].value.node, "42");
            }
            _ => panic!("expected Execute"),
        }
    }

    #[test]
    fn test_inspect_decision_basic() {
        let (parsed, diags) = parse("INSPECT DECISION Foo FROM a INTO b;");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::InspectDecision(insp) => {
                assert_eq!(insp.name.node, "Foo");
                assert_eq!(insp.from.node, "a");
                assert_eq!(insp.into.node, "b");
            }
            _ => panic!("expected InspectDecision"),
        }
    }

    #[test]
    fn test_describe_singular() {
        let (parsed, diags) = parse("DESCRIBE AGGREGATE Foo;");
        assert!(diags.is_empty());
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::Describe(desc) => match &desc.target {
                DescribeTarget::Single { concept, name } => {
                    assert_eq!(concept.node, ConceptType::Aggregate);
                    assert_eq!(name.node, "Foo");
                }
                _ => panic!("expected Single"),
            },
            _ => panic!("expected Describe"),
        }
    }

    #[test]
    fn test_apply_template_basic_int_param() {
        let (parsed, diags) = parse("APPLY TEMPLATE MyTpl WITH (x = 1);");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::ApplyTemplate(ut) => {
                assert_eq!(ut.name.node, "MyTpl");
                assert_eq!(ut.params.len(), 1);
                assert_eq!(ut.params[0].name.node, "x");
                match &ut.params[0].value.node {
                    TemplateArgValue::IntLit(v) => assert_eq!(*v, 1),
                    _ => panic!("expected IntLit"),
                }
            }
            _ => panic!("expected ApplyTemplate"),
        }
    }

    #[test]
    fn test_apply_template_basic() {
        let (parsed, diags) = parse("APPLY TEMPLATE MyTpl WITH (x = 1);");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::ApplyTemplate(ut) => {
                assert_eq!(ut.name.node, "MyTpl");
            }
            _ => panic!("expected ApplyTemplate"),
        }
    }

    #[test]
    fn test_error_recovery_multiple_statements() {
        // First statement is invalid, second is valid
        let (parsed, diags) = parse("FOOBAR; CREATE AGGREGATE Foo;");
        // First produces error, second parses successfully
        assert_eq!(parsed.statements.len(), 1);
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("unexpected token"));
    }

    #[test]
    fn test_synchronize_skips_to_semicolon() {
        // Invalid token followed by valid-looking statement
        let (parsed, diags) = parse("BADTOKEN stuff here; CREATE AGGREGATE Foo;");
        // Should get diagnostic for BADTOKEN, then recover and parse CREATE AGGREGATE
        assert!(!diags.is_empty());
        assert_eq!(parsed.statements.len(), 1);
    }

    #[test]
    fn test_create_or_missing_replace() {
        // CREATE OR <not REPLACE> should error
        let (_, diags) = parse("CREATE OR AGGREGATE Foo;");
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("expected Replace"));
    }

    #[test]
    fn test_parser_peek_at_end() {
        let parser = Parser::new(
            vec![Token {
                kind: TokenKind::Eof,
                lexeme: String::new(),
                span: Span { start: 0, end: 0 },
            }],
            String::new(),
        );
        assert!(parser.at_end());
        assert_eq!(parser.peek_kind(), TokenKind::Eof);
    }

    #[test]
    fn test_parser_check_without_consuming() {
        let tokens = vec![
            Token {
                kind: TokenKind::Create,
                lexeme: "CREATE".to_string(),
                span: Span { start: 0, end: 6 },
            },
            Token {
                kind: TokenKind::Eof,
                lexeme: String::new(),
                span: Span { start: 6, end: 6 },
            },
        ];
        let parser = Parser::new(tokens, String::new());
        assert!(parser.check(TokenKind::Create));
        assert!(!parser.check(TokenKind::Aggregate));
        // Position should not have changed
        assert_eq!(parser.pos, 0);
    }

    // -----------------------------------------------------------------------
    // Task 3.2: CREATE AGGREGATE tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_aggregate_with_fields() {
        let (parsed, diags) =
            parse("CREATE AGGREGATE Product (id UUID KEY, name STRING, price DECIMAL(10,2));");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::CreateAggregate(agg) => {
                assert_eq!(agg.name.node, "Product");
                assert!(!agg.or_replace);
                let fields = agg.fields.as_ref().unwrap();
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0].name.node, "id");
                assert_eq!(fields[0].data_type.node, DeqlType::Uuid);
                assert!(fields[0].is_key);
                assert_eq!(fields[1].name.node, "name");
                assert_eq!(fields[1].data_type.node, DeqlType::String);
                assert!(!fields[1].is_key);
                assert_eq!(fields[2].name.node, "price");
                assert_eq!(
                    fields[2].data_type.node,
                    DeqlType::Decimal {
                        precision: 10,
                        scale: 2
                    }
                );
            }
            _ => panic!("expected CreateAggregate"),
        }
    }

    #[test]
    fn test_create_or_replace_aggregate() {
        let (parsed, diags) = parse("CREATE OR REPLACE AGGREGATE Foo;");
        assert!(diags.is_empty());
        match &parsed.statements[0].node {
            DeqlStatement::CreateAggregate(agg) => {
                assert!(agg.or_replace);
                assert_eq!(agg.name.node, "Foo");
                assert!(agg.fields.is_none());
            }
            _ => panic!("expected CreateAggregate"),
        }
    }

    #[test]
    fn test_create_aggregate_all_types() {
        let (parsed, diags) = parse(
            "CREATE AGGREGATE AllTypes (a UUID, b STRING, c INT, d DECIMAL(5,3), e TIMESTAMP, f BOOLEAN);",
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateAggregate(agg) => {
                let fields = agg.fields.as_ref().unwrap();
                assert_eq!(fields.len(), 6);
                assert_eq!(fields[0].data_type.node, DeqlType::Uuid);
                assert_eq!(fields[1].data_type.node, DeqlType::String);
                assert_eq!(fields[2].data_type.node, DeqlType::Int);
                assert_eq!(
                    fields[3].data_type.node,
                    DeqlType::Decimal {
                        precision: 5,
                        scale: 3
                    }
                );
                assert_eq!(fields[4].data_type.node, DeqlType::Timestamp);
                assert_eq!(fields[5].data_type.node, DeqlType::Boolean);
            }
            _ => panic!("expected CreateAggregate"),
        }
    }

    #[test]
    fn test_create_aggregate_missing_name() {
        let (parsed, diags) = parse("CREATE AGGREGATE ;");
        assert!(parsed.statements.is_empty());
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("expected identifier"));
    }

    #[test]
    fn test_create_aggregate_unknown_type() {
        let (_, diags) = parse("CREATE AGGREGATE Foo (x BIGINT);");
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("unknown type"));
    }

    #[test]
    fn test_create_aggregate_type_case_insensitive() {
        let (parsed, diags) = parse("CREATE AGGREGATE Foo (id uuid, name string);");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateAggregate(agg) => {
                let fields = agg.fields.as_ref().unwrap();
                assert_eq!(fields[0].data_type.node, DeqlType::Uuid);
                assert_eq!(fields[1].data_type.node, DeqlType::String);
            }
            _ => panic!("expected CreateAggregate"),
        }
    }

    // -----------------------------------------------------------------------
    // Task 3.3: CREATE COMMAND and CREATE EVENT tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_command() {
        let (parsed, diags) =
            parse("CREATE COMMAND RegisterProduct (product_id UUID, name STRING, quantity INT);");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::CreateCommand(cmd) => {
                assert_eq!(cmd.name.node, "RegisterProduct");
                assert!(!cmd.or_replace);
                assert_eq!(cmd.fields.len(), 3);
            }
            _ => panic!("expected CreateCommand"),
        }
    }

    #[test]
    fn test_create_event() {
        let (parsed, diags) =
            parse("CREATE EVENT ProductRegistered (product_id UUID, name STRING);");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::CreateEvent(evt) => {
                assert_eq!(evt.name.node, "ProductRegistered");
                assert!(!evt.or_replace);
                assert_eq!(evt.fields.len(), 2);
            }
            _ => panic!("expected CreateEvent"),
        }
    }

    #[test]
    fn test_create_or_replace_event() {
        let (parsed, diags) = parse("CREATE OR REPLACE EVENT Foo (x UUID);");
        assert!(diags.is_empty());
        match &parsed.statements[0].node {
            DeqlStatement::CreateEvent(evt) => {
                assert!(evt.or_replace);
            }
            _ => panic!("expected CreateEvent"),
        }
    }

    #[test]
    fn test_create_command_missing_fields() {
        let (_, diags) = parse("CREATE COMMAND Foo;");
        assert!(!diags.is_empty());
        // Should error because field list is required (expects '(')
        assert!(diags[0].message.contains("expected LParen"));
    }

    // -----------------------------------------------------------------------
    // Task 3.4: EXECUTE tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_execute_multiple_assignments() {
        let (parsed, diags) = parse(
            "EXECUTE RegisterProduct(product_id := 'abc-123', name := 'Widget', quantity := 100);",
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::Execute(exec) => {
                assert_eq!(exec.command.node, "RegisterProduct");
                assert_eq!(exec.assignments.len(), 3);
                assert_eq!(exec.assignments[0].field.node, "product_id");
                assert_eq!(exec.assignments[0].value.node, "'abc-123'");
                assert_eq!(exec.assignments[1].field.node, "name");
                assert_eq!(exec.assignments[1].value.node, "'Widget'");
                assert_eq!(exec.assignments[2].field.node, "quantity");
                assert_eq!(exec.assignments[2].value.node, "100");
            }
            _ => panic!("expected Execute"),
        }
    }

    #[test]
    fn test_execute_empty_args() {
        let (parsed, diags) = parse("EXECUTE DoSomething();");
        assert!(diags.is_empty());
        match &parsed.statements[0].node {
            DeqlStatement::Execute(exec) => {
                assert_eq!(exec.command.node, "DoSomething");
                assert!(exec.assignments.is_empty());
            }
            _ => panic!("expected Execute"),
        }
    }

    #[test]
    fn test_execute_boolean_value() {
        let (parsed, diags) = parse("EXECUTE Cmd(active := true);");
        assert!(diags.is_empty());
        match &parsed.statements[0].node {
            DeqlStatement::Execute(exec) => {
                assert_eq!(exec.assignments[0].value.node, "true");
            }
            _ => panic!("expected Execute"),
        }
    }

    #[test]
    fn test_execute_decimal_value() {
        let (parsed, diags) = parse("EXECUTE Cmd(price := 19.99);");
        assert!(diags.is_empty());
        match &parsed.statements[0].node {
            DeqlStatement::Execute(exec) => {
                assert_eq!(exec.assignments[0].value.node, "19.99");
            }
            _ => panic!("expected Execute"),
        }
    }

    #[test]
    fn test_execute_missing_command_name() {
        let (_, diags) = parse("EXECUTE (x := 1);");
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("expected identifier"));
    }

    // -----------------------------------------------------------------------
    // Task 3.5: DESCRIBE tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_describe_all_singular_types() {
        let cases = vec![
            ("DESCRIBE AGGREGATE Foo;", ConceptType::Aggregate),
            ("DESCRIBE COMMAND Foo;", ConceptType::Command),
            ("DESCRIBE EVENT Foo;", ConceptType::Event),
            ("DESCRIBE DECISION Foo;", ConceptType::Decision),
            ("DESCRIBE PROJECTION Foo;", ConceptType::Projection),
            ("DESCRIBE EVENTSTORE Foo;", ConceptType::EventStore),
            ("DESCRIBE TEMPLATE Foo;", ConceptType::Template),
        ];
        for (input, expected_concept) in cases {
            let (parsed, diags) = parse(input);
            assert!(diags.is_empty(), "input: {}, diags: {:?}", input, diags);
            match &parsed.statements[0].node {
                DeqlStatement::Describe(desc) => match &desc.target {
                    DescribeTarget::Single { concept, name } => {
                        assert_eq!(concept.node, expected_concept, "input: {}", input);
                        assert_eq!(name.node, "Foo", "input: {}", input);
                    }
                    _ => panic!("expected Single for input: {}", input),
                },
                _ => panic!("expected Describe for input: {}", input),
            }
        }
    }

    #[test]
    fn test_describe_all_plural_types() {
        let cases = vec![
            ("DESCRIBE AGGREGATES;", PluralConceptType::Aggregates),
            ("DESCRIBE COMMANDS;", PluralConceptType::Commands),
            ("DESCRIBE EVENTS;", PluralConceptType::Events),
            ("DESCRIBE DECISIONS;", PluralConceptType::Decisions),
            ("DESCRIBE PROJECTIONS;", PluralConceptType::Projections),
            ("DESCRIBE EVENTSTORES;", PluralConceptType::EventStores),
            ("DESCRIBE TEMPLATES;", PluralConceptType::Templates),
        ];
        for (input, expected_concept) in cases {
            let (parsed, diags) = parse(input);
            assert!(diags.is_empty(), "input: {}, diags: {:?}", input, diags);
            match &parsed.statements[0].node {
                DeqlStatement::Describe(desc) => match &desc.target {
                    DescribeTarget::ListAll { concept } => {
                        assert_eq!(concept.node, expected_concept, "input: {}", input);
                    }
                    _ => panic!("expected ListAll for input: {}", input),
                },
                _ => panic!("expected Describe for input: {}", input),
            }
        }
    }

    #[test]
    fn test_describe_plural_case_insensitive() {
        let (parsed, diags) = parse("DESCRIBE aggregates;");
        assert!(diags.is_empty());
        match &parsed.statements[0].node {
            DeqlStatement::Describe(desc) => match &desc.target {
                DescribeTarget::ListAll { concept } => {
                    assert_eq!(concept.node, PluralConceptType::Aggregates);
                }
                _ => panic!("expected ListAll"),
            },
            _ => panic!("expected Describe"),
        }
    }

    #[test]
    fn test_describe_unknown_concept() {
        let (_, diags) = parse("DESCRIBE FOOBAR;");
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("expected concept type"));
    }

    // -----------------------------------------------------------------------
    // Multi-statement integration
    // -----------------------------------------------------------------------

    #[test]
    fn test_multiple_simple_statements() {
        let input = r#"
            CREATE AGGREGATE Product (id UUID KEY, name STRING);
            CREATE COMMAND RegisterProduct (product_id UUID, name STRING);
            CREATE EVENT ProductRegistered (product_id UUID, name STRING);
            EXECUTE RegisterProduct(product_id := 'abc', name := 'Widget');
            DESCRIBE AGGREGATES;
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 5);
    }

    // -----------------------------------------------------------------------
    // Task 5.1: CREATE DECISION tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_decision_simple_no_state() {
        let input = r#"
            CREATE DECISION Open
            FOR BankAccount
            ON COMMAND OpenAccount
            EMIT AS
                SELECT EVENT AccountOpened (
                    initial_balance := :initial_balance
                );
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert_eq!(dec.name.node, "Open");
                assert!(!dec.or_replace);
                assert_eq!(dec.aggregate.node, "BankAccount");
                assert_eq!(dec.command.node, "OpenAccount");
                assert!(dec.state_as.is_none());
                assert_eq!(dec.branches.len(), 1);
                assert_eq!(dec.branches[0].emit_items.len(), 1);
                assert_eq!(
                    dec.branches[0].emit_items[0].event_type.node,
                    "AccountOpened"
                );
                assert_eq!(dec.branches[0].emit_items[0].assignments.len(), 1);
                assert_eq!(
                    dec.branches[0].emit_items[0].assignments[0].field.node,
                    "initial_balance"
                );
                assert_eq!(
                    dec.branches[0].emit_items[0].assignments[0].value.node,
                    ":initial_balance"
                );
                assert!(dec.branches[0].guard.is_none());
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    #[test]
    fn test_create_decision_with_state_and_guard() {
        let input = r#"
            CREATE DECISION WithdrawMoney
            FOR BankAccount
            ON COMMAND Withdraw
            STATE AS
                SELECT balance FROM DeReg.BankAccount$Agg
                WHERE aggregate_id = :account_id
            EMIT AS
                SELECT EVENT MoneyWithdrawn (
                    amount        := :amount,
                    balance_after := balance - :amount
                )
                WHERE balance >= :amount;
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert_eq!(dec.name.node, "WithdrawMoney");
                assert_eq!(dec.aggregate.node, "BankAccount");
                assert_eq!(dec.command.node, "Withdraw");
                assert!(dec.state_as.is_some());
                let state = dec.state_as.as_ref().unwrap();
                assert!(state.sql.contains("balance"));
                assert!(state.sql.contains("DeReg"));
                assert_eq!(dec.branches.len(), 1);
                assert_eq!(dec.branches[0].emit_items.len(), 1);
                assert_eq!(
                    dec.branches[0].emit_items[0].event_type.node,
                    "MoneyWithdrawn"
                );
                assert_eq!(dec.branches[0].emit_items[0].assignments.len(), 2);
                assert!(dec.branches[0].guard.is_some());
                let guard = dec.branches[0].guard.as_ref().unwrap();
                assert!(guard.sql.contains("balance >= :amount"));
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    #[test]
    fn test_create_decision_as_shorthand() {
        let input = r#"
            CREATE DECISION Open
            FOR BankAccount
            ON COMMAND OpenAccount
            AS
                SELECT EVENT AccountOpened (
                    initial_balance := :initial_balance
                );
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert_eq!(dec.branches.len(), 1);
                assert_eq!(dec.branches[0].emit_items.len(), 1);
                assert_eq!(
                    dec.branches[0].emit_items[0].event_type.node,
                    "AccountOpened"
                );
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    #[test]
    fn test_create_decision_multiple_emit_items() {
        let input = r#"
            CREATE DECISION TransferFunds
            FOR MainWallet
            ON COMMAND TransferBetweenWallets
            STATE AS
                SELECT src.balance AS source_balance, dst.balance AS dest_balance
                FROM DeReg.MainWallet$Agg src
                JOIN DeReg.MainWallet$Agg dst ON dst.aggregate_id = :to_wallet
                WHERE src.aggregate_id = :from_wallet
            EMIT AS
                SELECT EVENT WalletTransferDebited (
                    amount := :amount,
                    balance_after := source_balance - :amount,
                    to_wallet := :to_wallet
                ),
                SELECT EVENT WalletTransferCredited (
                    amount := :amount,
                    balance_after := dest_balance + :amount,
                    from_wallet := :from_wallet
                )
                WHERE source_balance >= :amount;
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert_eq!(dec.branches.len(), 1);
                assert_eq!(dec.branches[0].emit_items.len(), 2);
                assert_eq!(
                    dec.branches[0].emit_items[0].event_type.node,
                    "WalletTransferDebited"
                );
                assert_eq!(
                    dec.branches[0].emit_items[1].event_type.node,
                    "WalletTransferCredited"
                );
                assert!(dec.branches[0].guard.is_some());
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    #[test]
    fn test_create_or_replace_decision() {
        let input = r#"
            CREATE OR REPLACE DECISION Open
            FOR BankAccount
            ON COMMAND OpenAccount
            EMIT AS
                SELECT EVENT AccountOpened (
                    initial_balance := :initial_balance
                );
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert!(dec.or_replace);
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    #[test]
    fn test_create_decision_missing_for() {
        let input = "CREATE DECISION Foo ON COMMAND Bar EMIT AS SELECT EVENT Baz ();";
        let (_, diags) = parse(input);
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("expected For"));
    }

    // -----------------------------------------------------------------------
    // Branching: UNION ALL syntax tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_decision_union_all_two_branches() {
        let input = r#"
            CREATE DECISION LoginAdmin
            FOR AdminAccount
            ON COMMAND LoginAdmin
            STATE AS
                SELECT COALESCE(current_hash, '') AS current_hash
                FROM DeReg."AdminAccount$Events"
                WHERE stream_id = :user_id
            EMIT AS
                SELECT EVENT AdminLoginSucceeded (
                    login_at := CURRENT_TIMESTAMP
                )
                WHERE current_hash IS NOT NULL AND UPPER(:password) = current_hash

                UNION ALL

                SELECT EVENT AdminLoginFailed (
                    attempted_at := CURRENT_TIMESTAMP,
                    reason := 'invalid_password'
                )
                WHERE current_hash IS NULL OR UPPER(:password) <> current_hash;
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert_eq!(dec.branches.len(), 2);

                // Branch 1
                assert_eq!(dec.branches[0].branch_index, 1);
                assert!(dec.branches[0].rule_name.is_none());
                assert_eq!(dec.branches[0].emit_items.len(), 1);
                assert_eq!(
                    dec.branches[0].emit_items[0].event_type.node,
                    "AdminLoginSucceeded"
                );
                assert!(dec.branches[0].guard.is_some());
                assert!(
                    dec.branches[0]
                        .guard
                        .as_ref()
                        .unwrap()
                        .sql
                        .contains("current_hash IS NOT NULL")
                );

                // Branch 2
                assert_eq!(dec.branches[1].branch_index, 2);
                assert!(dec.branches[1].rule_name.is_none());
                assert_eq!(dec.branches[1].emit_items.len(), 1);
                assert_eq!(
                    dec.branches[1].emit_items[0].event_type.node,
                    "AdminLoginFailed"
                );
                assert!(dec.branches[1].guard.is_some());
                assert!(
                    dec.branches[1]
                        .guard
                        .as_ref()
                        .unwrap()
                        .sql
                        .contains("current_hash IS NULL")
                );
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    #[test]
    fn test_create_decision_named_branches() {
        let input = r#"
            CREATE DECISION LoginAdmin
            FOR AdminAccount
            ON COMMAND LoginAdmin
            EMIT AS
                BRANCH PasswordMatch
                SELECT EVENT AdminLoginSucceeded (
                    login_at := CURRENT_TIMESTAMP
                )
                WHERE :password = 'correct'

                UNION ALL

                BRANCH PasswordMismatch
                SELECT EVENT AdminLoginFailed (
                    reason := 'bad_password'
                )
                WHERE :password <> 'correct';
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert_eq!(dec.branches.len(), 2);
                assert_eq!(
                    dec.branches[0].rule_name.as_ref().unwrap().node,
                    "PasswordMatch"
                );
                assert_eq!(
                    dec.branches[1].rule_name.as_ref().unwrap().node,
                    "PasswordMismatch"
                );
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    #[test]
    fn test_create_decision_three_branches() {
        let input = r#"
            CREATE DECISION ProcessPayment
            FOR Invoice
            ON COMMAND PaymentCommand
            EMIT AS
                BRANCH FullPayment
                SELECT EVENT PaymentCompleted (amount := :amount)
                WHERE :amount >= 100

                UNION ALL

                BRANCH PartialPayment
                SELECT EVENT PartialPaymentReceived (amount := :amount)
                WHERE :amount > 0 AND :amount < 100

                UNION ALL

                BRANCH NoPayment
                SELECT EVENT PaymentRejected (reason := 'zero_amount')
                WHERE :amount <= 0;
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert_eq!(dec.branches.len(), 3);
                assert_eq!(
                    dec.branches[0].rule_name.as_ref().unwrap().node,
                    "FullPayment"
                );
                assert_eq!(
                    dec.branches[1].rule_name.as_ref().unwrap().node,
                    "PartialPayment"
                );
                assert_eq!(
                    dec.branches[2].rule_name.as_ref().unwrap().node,
                    "NoPayment"
                );
                assert_eq!(
                    dec.branches[2].emit_items[0].event_type.node,
                    "PaymentRejected"
                );
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    #[test]
    fn test_create_decision_branch_with_comma_emits() {
        // Comma-separated emits inside a single branch, with UNION ALL for another branch
        let input = r#"
            CREATE DECISION ProcessOrder
            FOR SalesOrder
            ON COMMAND PlaceOrder
            EMIT AS
                SELECT EVENT OrderPlaced (id := :id),
                SELECT EVENT InventoryReserved (sku := :sku)
                WHERE :valid = TRUE

                UNION ALL

                SELECT EVENT OrderRejected (reason := 'invalid')
                WHERE :valid = FALSE;
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert_eq!(dec.branches.len(), 2);
                // First branch has two comma-separated emit items
                assert_eq!(dec.branches[0].emit_items.len(), 2);
                assert_eq!(dec.branches[0].emit_items[0].event_type.node, "OrderPlaced");
                assert_eq!(
                    dec.branches[0].emit_items[1].event_type.node,
                    "InventoryReserved"
                );
                assert!(dec.branches[0].guard.is_some());
                // Second branch has one emit item
                assert_eq!(dec.branches[1].emit_items.len(), 1);
                assert_eq!(
                    dec.branches[1].emit_items[0].event_type.node,
                    "OrderRejected"
                );
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    #[test]
    fn test_create_decision_duplicate_rule_names() {
        // Duplicate rule names are allowed per spec
        let input = r#"
            CREATE DECISION Test
            FOR Agg
            ON COMMAND Cmd
            EMIT AS
                BRANCH SameName
                SELECT EVENT EventA (x := :x)

                UNION ALL

                BRANCH SameName
                SELECT EVENT EventB (y := :y);
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert_eq!(dec.branches.len(), 2);
                assert_eq!(dec.branches[0].rule_name.as_ref().unwrap().node, "SameName");
                assert_eq!(dec.branches[1].rule_name.as_ref().unwrap().node, "SameName");
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    #[test]
    fn test_create_decision_no_union_all_backward_compat() {
        // Existing decisions without UNION ALL still parse as a single branch
        let input = r#"
            CREATE DECISION Simple
            FOR Agg
            ON COMMAND Cmd
            EMIT AS
                SELECT EVENT Happened (x := :x)
                WHERE :x > 0;
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateDecision(dec) => {
                assert_eq!(dec.branches.len(), 1);
                assert_eq!(dec.branches[0].branch_index, 1);
                assert!(dec.branches[0].rule_name.is_none());
                assert_eq!(dec.branches[0].emit_items.len(), 1);
                assert!(dec.branches[0].guard.is_some());
            }
            _ => panic!("expected CreateDecision"),
        }
    }

    // -----------------------------------------------------------------------
    // Task 5.2: CREATE PROJECTION tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_projection_simple() {
        let input = r#"
            CREATE PROJECTION StockLevels AS
            SELECT aggregate_id AS sku, SUM(data.quantity) AS quantity
            FROM DeReg.Inventory$Events
            GROUP BY aggregate_id;
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::CreateProjection(proj) => {
                assert_eq!(proj.name.node, "StockLevels");
                assert!(!proj.or_replace);
                assert!(proj.body.sql.contains("SELECT"));
                assert!(proj.body.sql.contains("aggregate_id"));
                assert!(proj.body.sql.contains("GROUP BY"));
            }
            _ => panic!("expected CreateProjection"),
        }
    }

    #[test]
    fn test_create_or_replace_projection() {
        let input = "CREATE OR REPLACE PROJECTION Foo AS SELECT 1;";
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateProjection(proj) => {
                assert!(proj.or_replace);
                assert_eq!(proj.name.node, "Foo");
            }
            _ => panic!("expected CreateProjection"),
        }
    }

    #[test]
    fn test_create_projection_with_subquery() {
        let input = r#"
            CREATE PROJECTION PendingReorders AS
            SELECT aggregate_id AS sku
            FROM DeReg.Inventory$Events
            WHERE event_type = 'ReorderTriggered'
              AND aggregate_id NOT IN (
                  SELECT aggregate_id
                  FROM DeReg.Inventory$Events
                  WHERE event_type = 'ReorderFulfilled'
              )
            GROUP BY aggregate_id;
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateProjection(proj) => {
                assert!(proj.body.sql.contains("NOT IN"));
                assert!(proj.body.sql.contains("ReorderFulfilled"));
            }
            _ => panic!("expected CreateProjection"),
        }
    }

    // -----------------------------------------------------------------------
    // Task 5.3: CREATE EVENTSTORE tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_eventstore_basic() {
        let input = r#"
            CREATE EVENTSTORE inventory_local
            WITH (
                envelope.event_id_key = 'event_id',
                durable.type = 'parquet',
                durable.compression = 'snappy',
                partition.by = ('dt','stream_type'),
                partition.enforce = true,
                row_group.target_mb = 64,
                strict.immutable_events = true
            );
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::CreateEventStore(es) => {
                assert_eq!(es.name.node, "inventory_local");
                assert!(!es.or_replace);
                assert!(es.config.len() >= 7);
                // Check dotted key
                assert_eq!(es.config[0].key.node, "envelope.event_id_key");
                match &es.config[0].value.node {
                    ConfigValue::StringLit(s) => assert_eq!(s, "event_id"),
                    _ => panic!("expected StringLit"),
                }
                // Check list value
                let partition_by = es
                    .config
                    .iter()
                    .find(|c| c.key.node == "partition.by")
                    .unwrap();
                match &partition_by.value.node {
                    ConfigValue::List(items) => {
                        assert_eq!(items.len(), 2);
                        assert_eq!(items[0], "dt");
                        assert_eq!(items[1], "stream_type");
                    }
                    _ => panic!("expected List"),
                }
                // Check boolean
                let enforce = es
                    .config
                    .iter()
                    .find(|c| c.key.node == "partition.enforce")
                    .unwrap();
                match &enforce.value.node {
                    ConfigValue::BoolLit(b) => assert!(*b),
                    _ => panic!("expected BoolLit"),
                }
                // Check integer
                let target_mb = es
                    .config
                    .iter()
                    .find(|c| c.key.node == "row_group.target_mb")
                    .unwrap();
                match &target_mb.value.node {
                    ConfigValue::IntLit(v) => assert_eq!(*v, 64),
                    _ => panic!("expected IntLit"),
                }
            }
            _ => panic!("expected CreateEventStore"),
        }
    }

    #[test]
    fn test_create_or_replace_eventstore() {
        let input = "CREATE OR REPLACE EVENTSTORE foo WITH (x = 1);";
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateEventStore(es) => {
                assert!(es.or_replace);
            }
            _ => panic!("expected CreateEventStore"),
        }
    }

    #[test]
    fn test_create_eventstore_no_with_is_valid() {
        let (parsed, diags) = parse("CREATE EVENTSTORE foo;");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateEventStore(es) => {
                assert_eq!(es.name.node, "foo");
                assert!(es.config.is_empty());
            }
            _ => panic!("expected CreateEventStore"),
        }
    }

    #[test]
    fn test_create_eventstore_missing_with_before_paren() {
        // Parentheses without WITH keyword is a parse error
        let (_, diags) = parse("CREATE EVENTSTORE foo (x = 1);");
        assert!(!diags.is_empty());
    }

    // -----------------------------------------------------------------------
    // Task 5.4: CREATE TEMPLATE tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_create_template_untyped_params_paren_body() {
        let input = r#"
            CREATE TEMPLATE SoftDeletable (EntityName) AS (
                CREATE COMMAND Delete{{EntityName}} (
                    id     UUID,
                    reason STRING
                );
            );
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::CreateTemplate(tpl) => {
                assert_eq!(tpl.name.node, "SoftDeletable");
                assert!(!tpl.or_replace);
                assert_eq!(tpl.params.len(), 1);
                assert_eq!(tpl.params[0].name.node, "EntityName");
                assert!(tpl.params[0].data_type.is_none());
                assert!(tpl.raw_body.is_some());
                let body = tpl.raw_body.as_ref().unwrap();
                // Token-based reconstruction adds spaces between tokens
                assert!(body.contains("Delete"));
                assert!(body.contains("{{EntityName}}"));
            }
            _ => panic!("expected CreateTemplate"),
        }
    }

    #[test]
    fn test_create_template_typed_params_no_paren_body() {
        let input = r#"
            CREATE TEMPLATE wallet_aggregate
            WITH (
                wallet_name STRING,
                currency    STRING
            )
            AS
                CREATE AGGREGATE {{wallet_name}}Wallet (
                    wallet_id UUID KEY,
                    currency  STRING,
                    balance   DECIMAL(12,2)
                );
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateTemplate(tpl) => {
                assert_eq!(tpl.name.node, "wallet_aggregate");
                assert_eq!(tpl.params.len(), 2);
                assert_eq!(tpl.params[0].name.node, "wallet_name");
                assert_eq!(
                    tpl.params[0].data_type.as_ref().unwrap().node,
                    DeqlType::String
                );
                assert_eq!(tpl.params[1].name.node, "currency");
                assert!(tpl.raw_body.is_some());
            }
            _ => panic!("expected CreateTemplate"),
        }
    }

    #[test]
    fn test_create_or_replace_template() {
        let input = "CREATE OR REPLACE TEMPLATE Foo (X) AS (CREATE AGGREGATE {{X}};);";
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::CreateTemplate(tpl) => {
                assert!(tpl.or_replace);
            }
            _ => panic!("expected CreateTemplate"),
        }
    }

    #[test]
    fn test_create_template_empty_params_error() {
        let (_, diags) = parse("CREATE TEMPLATE Foo () AS (CREATE AGGREGATE Bar;);");
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("must not be empty"));
    }

    // -----------------------------------------------------------------------
    // Task 5.5: INSPECT tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_inspect_projection_basic() {
        let (parsed, diags) =
            parse("INSPECT PROJECTION StockLevels FROM all_events INTO stock_levels;");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::InspectProjection(insp) => {
                assert_eq!(insp.name.node, "StockLevels");
                assert_eq!(insp.from.node, "all_events");
                assert_eq!(insp.into.node, "stock_levels");
                assert!(insp.offset.is_none());
                assert!(insp.guard.is_none());
                assert!(insp.limit.is_none());
            }
            _ => panic!("expected InspectProjection"),
        }
    }

    #[test]
    fn test_inspect_decision_dotted_refs() {
        let (parsed, diags) = parse(
            "INSPECT DECISION RegisterNewProduct FROM test_register_products INTO simulated_registrations;",
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::InspectDecision(insp) => {
                assert_eq!(insp.name.node, "RegisterNewProduct");
                assert_eq!(insp.from.node, "test_register_products");
                assert_eq!(insp.into.node, "simulated_registrations");
            }
            _ => panic!("expected InspectDecision"),
        }
    }

    #[test]
    fn test_inspect_with_dereg_refs() {
        let (parsed, diags) = parse(
            "INSPECT PROJECTION StockLevels FROM DeReg.Inventory$Events INTO DeReg.StockLevels;",
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::InspectProjection(insp) => {
                assert_eq!(insp.from.node, "DeReg.Inventory$Events");
                assert_eq!(insp.into.node, "DeReg.StockLevels");
            }
            _ => panic!("expected InspectProjection"),
        }
    }

    #[test]
    fn test_inspect_with_offset() {
        let (parsed, diags) = parse(
            "INSPECT PROJECTION StockLevels FROM DeReg.Inventory$Events INTO DeReg.StockLevels OFFSET 50000;",
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::InspectProjection(insp) => {
                assert_eq!(insp.offset.as_ref().unwrap().node, 50000);
            }
            _ => panic!("expected InspectProjection"),
        }
    }

    #[test]
    fn test_inspect_with_where() {
        let (parsed, diags) = parse(
            "INSPECT PROJECTION StockLevels FROM DeReg.Inventory$Events INTO DeReg.StockLevels WHERE aggregate_id = 'BOLT-M8-50';",
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::InspectProjection(insp) => {
                assert!(insp.guard.is_some());
                assert!(insp.guard.as_ref().unwrap().sql.contains("aggregate_id"));
            }
            _ => panic!("expected InspectProjection"),
        }
    }

    #[test]
    fn test_inspect_with_offset_and_limit() {
        let (parsed, diags) = parse(
            "INSPECT PROJECTION StockLevels FROM DeReg.Inventory$Events INTO DeReg.StockLevels OFFSET 0 LIMIT 10000;",
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::InspectProjection(insp) => {
                assert_eq!(insp.offset.as_ref().unwrap().node, 0);
                assert_eq!(insp.limit.as_ref().unwrap().node, 10000);
            }
            _ => panic!("expected InspectProjection"),
        }
    }

    #[test]
    fn test_inspect_with_all_clauses() {
        let (parsed, diags) = parse(
            "INSPECT PROJECTION StockLevels FROM DeReg.Inventory$Events INTO DeReg.StockLevels OFFSET 25000 WHERE data.warehouse = 'WH-EAST' LIMIT 5000;",
        );
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::InspectProjection(insp) => {
                assert_eq!(insp.offset.as_ref().unwrap().node, 25000);
                assert!(insp.guard.is_some());
                assert_eq!(insp.limit.as_ref().unwrap().node, 5000);
            }
            _ => panic!("expected InspectProjection"),
        }
    }

    #[test]
    fn test_inspect_missing_from() {
        let (_, diags) = parse("INSPECT DECISION Foo INTO bar;");
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("expected From"));
    }

    // -----------------------------------------------------------------------
    // Task 5.6: APPLY TEMPLATE tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_apply_template_string_param() {
        let input = r#"
            APPLY TEMPLATE SoftDeletable WITH (
                EntityName = 'Inventory'
            );
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::ApplyTemplate(ut) => {
                assert_eq!(ut.name.node, "SoftDeletable");
                assert_eq!(ut.params.len(), 1);
                assert_eq!(ut.params[0].name.node, "EntityName");
                match &ut.params[0].value.node {
                    TemplateArgValue::StringLit(s) => assert_eq!(s, "Inventory"),
                    _ => panic!("expected StringLit"),
                }
            }
            _ => panic!("expected ApplyTemplate"),
        }
    }

    #[test]
    fn test_apply_template_with_field_list() {
        let input = r#"
            APPLY TEMPLATE RegistryEntity WITH (
                EntityName = 'Citizen',
                Fields = (
                    full_name STRING,
                    date_of_birth STRING,
                    gender STRING
                )
            );
        "#;
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::ApplyTemplate(ut) => {
                assert_eq!(ut.name.node, "RegistryEntity");
                assert_eq!(ut.params.len(), 2);
                assert_eq!(ut.params[0].name.node, "EntityName");
                match &ut.params[1].value.node {
                    TemplateArgValue::FieldList(fields) => {
                        assert_eq!(fields.len(), 3);
                        assert_eq!(fields[0].name.node, "full_name");
                        assert_eq!(fields[0].data_type.node, DeqlType::String);
                    }
                    _ => panic!("expected FieldList"),
                }
            }
            _ => panic!("expected ApplyTemplate"),
        }
    }

    #[test]
    fn test_apply_template_multiple_params() {
        let input =
            "APPLY TEMPLATE wallet_aggregate WITH (wallet_name = 'Main', currency = 'USD');";
        let (parsed, diags) = parse(input);
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        match &parsed.statements[0].node {
            DeqlStatement::ApplyTemplate(ut) => {
                assert_eq!(ut.name.node, "wallet_aggregate");
                assert_eq!(ut.params.len(), 2);
                assert_eq!(ut.params[0].name.node, "wallet_name");
                assert_eq!(ut.params[1].name.node, "currency");
            }
            _ => panic!("expected ApplyTemplate"),
        }
    }

    #[test]
    fn test_use_template_produces_helpful_error() {
        let (_, diags) = parse("USE TEMPLATE Foo WITH (x = 1);");
        assert!(!diags.is_empty());
        assert!(
            diags[0].message.contains("APPLY TEMPLATE"),
            "expected helpful error suggesting APPLY TEMPLATE, got: {}",
            diags[0].message
        );
    }

    #[test]
    fn test_apply_template_missing_with() {
        let (_, diags) = parse("APPLY TEMPLATE Foo (x = 1);");
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("expected With"));
    }

    // -----------------------------------------------------------------------
    // EXPORT DEREG and VALIDATE DEREG tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_export_dereg_no_path() {
        let (parsed, diags) = parse("EXPORT DEREG;");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::ExportDeReg(e) => {
                assert!(e.path.is_none());
            }
            _ => panic!("expected ExportDeReg"),
        }
    }

    #[test]
    fn test_export_dereg_with_path() {
        let (parsed, diags) = parse("EXPORT DEREG TO 'output.deql';");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        match &parsed.statements[0].node {
            DeqlStatement::ExportDeReg(e) => {
                assert_eq!(e.path.as_ref().unwrap().node, "output.deql");
            }
            _ => panic!("expected ExportDeReg"),
        }
    }

    #[test]
    fn test_export_dereg_case_insensitive() {
        let (parsed, diags) = parse("export dereg;");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        assert!(matches!(
            &parsed.statements[0].node,
            DeqlStatement::ExportDeReg(_)
        ));
    }

    #[test]
    fn test_validate_dereg() {
        let (parsed, diags) = parse("VALIDATE DEREG;");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        assert!(matches!(
            &parsed.statements[0].node,
            DeqlStatement::ValidateDeReg
        ));
    }

    #[test]
    fn test_validate_dereg_case_insensitive() {
        let (parsed, diags) = parse("validate dereg;");
        assert!(diags.is_empty(), "diagnostics: {:?}", diags);
        assert_eq!(parsed.statements.len(), 1);
        assert!(matches!(
            &parsed.statements[0].node,
            DeqlStatement::ValidateDeReg
        ));
    }
}
