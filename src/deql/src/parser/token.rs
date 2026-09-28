// Token types for the DeQL lexer.

/// Byte-offset range in source text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    /// Returns a span covering both `self` and `other`.
    pub fn merge(self, other: Span) -> Span {
        Span {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }
}

/// A token with its kind, lexeme, and source location.
#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub lexeme: String,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    // Keywords (case-insensitive)
    Create,
    Or,
    Replace,
    Aggregate,
    Command,
    Event,
    Decision,
    Projection,
    Inspection,
    Eventstore,
    Template,
    For,
    On,
    State,
    As,
    Emit,
    Select,
    From,
    Where,
    Group,
    By,
    Order,
    Desc,
    Asc,
    With,
    Execute,
    Inspect,
    Describe,
    Apply,
    Into,
    Offset,
    Limit,
    In,
    And,
    Not,
    Is,
    Null,
    True,
    False,
    Case,
    When,
    Then,
    Else,
    End,
    Join,
    Left,
    Right,
    Inner,
    Outer,
    Union,
    All,
    Branch,
    Key,
    Interval,
    DeReg,

    // Literals and identifiers
    Identifier,
    QuotedIdentifier,
    StringLiteral,
    IntegerLiteral,
    DecimalLiteral,

    // Special tokens
    BindParam,
    TemplatePlaceholder,

    // Operators and punctuation
    ColonEquals, // :=
    LParen,      // (
    RParen,      // )
    Comma,       // ,
    Semicolon,   // ;
    Dot,         // .
    Dollar,      // $
    Eq,          // =
    NotEq,       // <> or !=
    Lt,          // <
    Gt,          // >
    LtEq,        // <=
    GtEq,        // >=
    Plus,        // +
    Minus,       // -
    Star,        // *
    Slash,       // /
    Pipe2,       // ||

    // End of input
    Eof,

    // Other
    Sensitive,
    Volatile,
}
