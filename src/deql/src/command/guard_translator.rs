use std::{collections::HashSet, fmt};

use datafusion::{
    arrow::datatypes::Schema,
    common::ScalarValue,
    logical_expr::Expr,
    prelude::{col, lit},
};

/// Errors that can occur while translating DeQL guard expressions into DataFusion `Expr`.
#[derive(Debug)]
pub enum TranslateError {
    NotImplemented(String),
    Unsupported(String),
    DataFusion(String),
}

impl fmt::Display for TranslateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TranslateError::NotImplemented(s) => write!(f, "not implemented: {}", s),
            TranslateError::Unsupported(s) => write!(f, "unsupported: {}", s),
            TranslateError::DataFusion(s) => write!(f, "datafusion error: {}", s),
        }
    }
}

impl std::error::Error for TranslateError {}

// --- Very small tokenizer / parser for a restricted expression grammar ---

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Number(String),
    Str(String),
    Op(String),
    LParen,
    RParen,
    And,
    Or,
    Not,
    Null,
    True,
    False,
}

fn tokenize(s: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut it = s.chars().peekable();
    while let Some(&ch) = it.peek() {
        if ch.is_whitespace() {
            it.next();
            continue;
        }
        if ch == '(' {
            out.push(Token::LParen);
            it.next();
            continue;
        }
        if ch == ')' {
            out.push(Token::RParen);
            it.next();
            continue;
        }
        if ch == '\'' {
            // parse single-quoted string
            it.next();
            let mut buf = String::new();
            while let Some(&c2) = it.peek() {
                it.next();
                if c2 == '\'' {
                    break;
                }
                buf.push(c2);
            }
            out.push(Token::Str(buf));
            continue;
        }
        // two-char ops
        if let Some(next) = { let mut clone = it.clone(); clone.nth(1) } {
            let two = format!("{}{}", ch, next);
            if two == "<>" || two == "<=" || two == ">=" || two == "!=" {
                out.push(Token::Op(two));
                it.next();
                it.next();
                continue;
            }
        }
        // one-char ops
        if ch == '=' || ch == '<' || ch == '>' || ch == '!' {
            out.push(Token::Op(ch.to_string()));
            it.next();
            continue;
        }
        if ch.is_ascii_digit() || (ch == '-' && it.clone().nth(1).map(|c| c.is_ascii_digit()).unwrap_or(false)) {
            let mut buf = String::new();
            if ch == '-' {
                buf.push('-');
                it.next();
            }
            while let Some(&c2) = it.peek() {
                if c2.is_ascii_digit() || c2 == '.' {
                    buf.push(c2);
                    it.next();
                } else {
                    break;
                }
            }
            out.push(Token::Number(buf));
            continue;
        }
        // identifiers / keywords
        if ch.is_alphanumeric() || ch == '_' {
            let mut buf = String::new();
            while let Some(&c2) = it.peek() {
                if c2.is_alphanumeric() || c2 == '_' || c2 == '$' {
                    buf.push(c2);
                    it.next();
                } else {
                    break;
                }
            }
            let up = buf.to_uppercase();
            match up.as_str() {
                "AND" => out.push(Token::And),
                "OR" => out.push(Token::Or),
                "NOT" => out.push(Token::Not),
                "NULL" => out.push(Token::Null),
                "TRUE" => out.push(Token::True),
                "FALSE" => out.push(Token::False),
                _ => out.push(Token::Ident(buf)),
            }
            continue;
        }

        // unknown char: skip
        it.next();
    }
    out
}

struct Parser {
    toks: Vec<Token>,
    pos: usize,
    refs: HashSet<String>,
}

impl Parser {
    fn new(toks: Vec<Token>) -> Self {
        Self { toks, pos: 0, refs: HashSet::new() }
    }

    fn peek(&self) -> Option<&Token> {
        self.toks.get(self.pos)
    }
    fn bump(&mut self) -> Option<Token> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() { self.pos += 1; }
        t
    }

    fn parse(&mut self) -> Result<Expr, TranslateError> {
        let e = self.parse_or()?;
        Ok(e)
    }

    fn parse_or(&mut self) -> Result<Expr, TranslateError> {
        let mut left = self.parse_and()?;
        while let Some(Token::Or) = self.peek() {
            self.bump();
            let right = self.parse_and()?;
            left = left.or(right);
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, TranslateError> {
        let mut left = self.parse_not()?;
        while let Some(Token::And) = self.peek() {
            self.bump();
            let right = self.parse_not()?;
            left = left.and(right);
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, TranslateError> {
        if let Some(Token::Not) = self.peek() {
            self.bump();
            let e = self.parse_not()?;
            return Ok(Expr::Not(Box::new(e)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<Expr, TranslateError> {
        match self.peek() {
            Some(Token::LParen) => {
                self.bump();
                let e = self.parse()?;
                if let Some(Token::RParen) = self.peek() {
                    self.bump();
                }
                Ok(e)
            }
            _ => self.parse_comparison(),
        }
    }

    fn parse_comparison(&mut self) -> Result<Expr, TranslateError> {
        let left = self.parse_operand()?;
        if let Some(tok) = self.peek() {
            if let Token::Op(op) = tok {
                let op = op.clone();
                self.bump();
                let right = self.parse_operand()?;
                // map ops to Expr methods
                let res = match op.as_str() {
                    "=" => left.eq(right),
                    "==" => left.eq(right),
                    "!=" => left.not_eq(right),
                    "<>" => left.not_eq(right),
                    "<" => left.lt(right),
                    "<=" => left.lt_eq(right),
                    ">" => left.gt(right),
                    ">=" => left.gt_eq(right),
                    _ => return Err(TranslateError::Unsupported(format!("operator {}", op))),
                };
                return Ok(res);
            }
        }
        Ok(left)
    }

    fn parse_operand(&mut self) -> Result<Expr, TranslateError> {
        match self.bump() {
            Some(Token::Ident(name)) => {
                self.refs.insert(name.clone());
                Ok(col(name.as_str()))
            }
            Some(Token::Number(n)) => {
                if n.contains('.') {
                    match n.parse::<f64>() {
                        Ok(f) => Ok(lit(f)),
                        Err(_) => Err(TranslateError::DataFusion(format!("bad number {}", n))),
                    }
                } else {
                    match n.parse::<i64>() {
                        Ok(i) => Ok(lit(i)),
                        Err(_) => Err(TranslateError::DataFusion(format!("bad int {}", n))),
                    }
                }
            }
            Some(Token::Str(s)) => Ok(lit(s)),
            Some(Token::True) => Ok(lit(true)),
            Some(Token::False) => Ok(lit(false)),
            Some(Token::Null) => Ok(Expr::Literal(ScalarValue::Utf8(None), None)),
            Some(tok) => Err(TranslateError::Unsupported(format!("unexpected token: {:?}", tok))),
            None => Err(TranslateError::Unsupported("unexpected end of input".to_string())),
        }
    }
}

/// Translate a DeQL guard (given as a string for now) into a DataFusion `Expr`.
///
/// Minimal implementation: identifiers, string/number/boolean/null literals,
/// comparison operators (=, !=, <>, <, <=, >, >=) and boolean `AND`/`OR`/`NOT`.
pub fn translate_guard(guard_sql: &str, _schema: &Schema) -> Result<Expr, TranslateError> {
    let toks = tokenize(guard_sql);
    let mut p = Parser::new(toks);
    let expr = p.parse()?;
    Ok(expr)
}

/// Return the set of referenced fields in the guard expression.
pub fn referenced_fields(guard_sql: &str) -> HashSet<String> {
    let toks = tokenize(guard_sql);
    let mut p = Parser::new(toks);
    // parse but ignore errors — we still want refs collected where possible
    let _ = p.parse();
    p.refs
}

/// Validate whether the guard expression uses only supported constructs.
pub fn validate_guard_supported(guard_sql: &str) -> Result<(), TranslateError> {
    let toks = tokenize(guard_sql);
    let mut p = Parser::new(toks);
    // If parsing succeeds, it's supported by this minimal translator.
    p.parse().map(|_| ())
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn basic_compile_checks() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let res = translate_guard("a = 1", &schema);
        assert!(res.is_ok());
        let refs = referenced_fields("a = 1");
        assert!(refs.contains("a"));
        assert!(validate_guard_supported("a = 1").is_ok());
    }

    #[test]
    fn boolean_and_or_not() {
        let schema = Schema::new(vec![Field::new("x", DataType::Int64, true), Field::new("y", DataType::Int64, true)]);
        let res = translate_guard("x = 1 AND NOT (y = 2 OR y = 3)", &schema);
        assert!(res.is_ok());
        let refs = referenced_fields("x = 1 AND NOT (y = 2 OR y = 3)");
        assert!(refs.contains("x"));
        assert!(refs.contains("y"));
    }
}
