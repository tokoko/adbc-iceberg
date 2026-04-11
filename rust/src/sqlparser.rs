/// Restricted SQL parser: SELECT <columns|*> FROM [schema.]table [WHERE <filter>]

#[derive(Debug)]
pub struct SelectStatement {
    pub select_all: bool,
    pub columns: Vec<String>,
    pub schema: Option<String>,
    pub table: String,
    pub where_clause: Option<Expr>,
}

#[derive(Debug)]
pub enum Expr {
    Binary {
        column: String,
        op: BinaryOp,
        value: LiteralValue,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull(String),
    IsNotNull(String),
}

#[derive(Debug)]
pub enum BinaryOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug)]
pub enum LiteralValue {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    StringLit(String),
    Number(String),
    Comma,
    Dot,
    Star,
    LParen,
    RParen,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    // Keywords
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    Is,
    Null,
    Between,
    In,
    True,
    False,
    Eof,
}

fn lex(input: &str) -> Result<Vec<Token>, String> {
    let bytes = input.as_bytes();
    let mut pos = 0;
    let mut tokens = Vec::new();

    while pos < bytes.len() {
        let ch = bytes[pos];
        if ch.is_ascii_whitespace() {
            pos += 1;
            continue;
        }
        match ch {
            b',' => { tokens.push(Token::Comma); pos += 1; }
            b'.' => { tokens.push(Token::Dot); pos += 1; }
            b'*' => { tokens.push(Token::Star); pos += 1; }
            b'(' => { tokens.push(Token::LParen); pos += 1; }
            b')' => { tokens.push(Token::RParen); pos += 1; }
            b'=' => { tokens.push(Token::Eq); pos += 1; }
            b'<' => {
                if pos + 1 < bytes.len() && bytes[pos + 1] == b'=' {
                    tokens.push(Token::Lte); pos += 2;
                } else if pos + 1 < bytes.len() && bytes[pos + 1] == b'>' {
                    tokens.push(Token::Neq); pos += 2;
                } else {
                    tokens.push(Token::Lt); pos += 1;
                }
            }
            b'>' => {
                if pos + 1 < bytes.len() && bytes[pos + 1] == b'=' {
                    tokens.push(Token::Gte); pos += 2;
                } else {
                    tokens.push(Token::Gt); pos += 1;
                }
            }
            b'!' => {
                if pos + 1 < bytes.len() && bytes[pos + 1] == b'=' {
                    tokens.push(Token::Neq); pos += 2;
                } else {
                    return Err(format!("unexpected '!' at position {pos}"));
                }
            }
            b'\'' => {
                pos += 1;
                let start = pos;
                let mut s = String::new();
                while pos < bytes.len() {
                    if bytes[pos] == b'\'' {
                        if pos + 1 < bytes.len() && bytes[pos + 1] == b'\'' {
                            s.push_str(&input[start..pos]);
                            s.push('\'');
                            pos += 2;
                            continue;
                        }
                        if s.is_empty() {
                            s = input[start..pos].to_string();
                        } else {
                            s.push_str(&input[start..pos]);
                        }
                        pos += 1;
                        break;
                    }
                    pos += 1;
                }
                tokens.push(Token::StringLit(s));
            }
            b'-' if pos + 1 < bytes.len() && bytes[pos + 1].is_ascii_digit() => {
                let start = pos;
                pos += 1;
                let mut is_float = false;
                while pos < bytes.len() && (bytes[pos].is_ascii_digit() || (bytes[pos] == b'.' && !is_float)) {
                    if bytes[pos] == b'.' { is_float = true; }
                    pos += 1;
                }
                tokens.push(Token::Number(input[start..pos].to_string()));
            }
            _ if ch.is_ascii_digit() => {
                let start = pos;
                let mut is_float = false;
                while pos < bytes.len() && (bytes[pos].is_ascii_digit() || (bytes[pos] == b'.' && !is_float)) {
                    if bytes[pos] == b'.' { is_float = true; }
                    pos += 1;
                }
                tokens.push(Token::Number(input[start..pos].to_string()));
            }
            _ if ch.is_ascii_alphabetic() || ch == b'_' => {
                let start = pos;
                while pos < bytes.len() && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
                    pos += 1;
                }
                let word = &input[start..pos];
                let tok = match word.to_uppercase().as_str() {
                    "SELECT" => Token::Select,
                    "FROM" => Token::From,
                    "WHERE" => Token::Where,
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    "NOT" => Token::Not,
                    "IS" => Token::Is,
                    "NULL" => Token::Null,
                    "BETWEEN" => Token::Between,
                    "IN" => Token::In,
                    "TRUE" => Token::True,
                    "FALSE" => Token::False,
                    _ => Token::Ident(word.to_string()),
                };
                tokens.push(tok);
            }
            _ => return Err(format!("unexpected character '{ch}' at position {pos}")),
        }
    }
    tokens.push(Token::Eof);
    Ok(tokens)
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<(), String> {
        let tok = self.advance();
        if std::mem::discriminant(&tok) == std::mem::discriminant(expected) {
            Ok(())
        } else {
            Err(format!("expected {expected:?}, got {tok:?}"))
        }
    }
}

pub fn parse(sql: &str) -> Result<SelectStatement, String> {
    let tokens = lex(sql)?;
    let mut p = Parser { tokens, pos: 0 };

    p.expect(&Token::Select)?;

    // Columns
    let (select_all, columns) = if *p.peek() == Token::Star {
        p.advance();
        (true, vec![])
    } else {
        let mut cols = Vec::new();
        loop {
            match p.advance() {
                Token::Ident(name) => cols.push(name),
                other => return Err(format!("expected column name, got {other:?}")),
            }
            if *p.peek() != Token::Comma {
                break;
            }
            p.advance();
        }
        (false, cols)
    };

    p.expect(&Token::From)?;

    // Table reference
    let first = match p.advance() {
        Token::Ident(name) => name,
        other => return Err(format!("expected table name, got {other:?}")),
    };
    let (schema, table) = if *p.peek() == Token::Dot {
        p.advance();
        match p.advance() {
            Token::Ident(name) => (Some(first), name),
            other => return Err(format!("expected table name after dot, got {other:?}")),
        }
    } else {
        (None, first)
    };

    // WHERE
    let where_clause = if *p.peek() == Token::Where {
        p.advance();
        Some(parse_or(&mut p)?)
    } else {
        None
    };

    Ok(SelectStatement {
        select_all,
        columns,
        schema,
        table,
        where_clause,
    })
}

fn parse_or(p: &mut Parser) -> Result<Expr, String> {
    let mut left = parse_and(p)?;
    while *p.peek() == Token::Or {
        p.advance();
        let right = parse_and(p)?;
        left = Expr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and(p: &mut Parser) -> Result<Expr, String> {
    let mut left = parse_unary(p)?;
    while *p.peek() == Token::And {
        p.advance();
        let right = parse_unary(p)?;
        left = Expr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_unary(p: &mut Parser) -> Result<Expr, String> {
    if *p.peek() == Token::Not {
        p.advance();
        let inner = parse_unary(p)?;
        return Ok(Expr::Not(Box::new(inner)));
    }
    parse_primary(p)
}

fn parse_primary(p: &mut Parser) -> Result<Expr, String> {
    if *p.peek() == Token::LParen {
        p.advance();
        let expr = parse_or(p)?;
        p.expect(&Token::RParen)?;
        return Ok(expr);
    }

    let column = match p.advance() {
        Token::Ident(name) => name,
        other => return Err(format!("expected column name, got {other:?}")),
    };

    // IS [NOT] NULL
    if *p.peek() == Token::Is {
        p.advance();
        if *p.peek() == Token::Not {
            p.advance();
            p.expect(&Token::Null)?;
            return Ok(Expr::IsNotNull(column));
        }
        p.expect(&Token::Null)?;
        return Ok(Expr::IsNull(column));
    }

    // Comparison
    let op = match p.advance() {
        Token::Eq => BinaryOp::Eq,
        Token::Neq => BinaryOp::Neq,
        Token::Lt => BinaryOp::Lt,
        Token::Lte => BinaryOp::Lte,
        Token::Gt => BinaryOp::Gt,
        Token::Gte => BinaryOp::Gte,
        other => return Err(format!("expected comparison operator, got {other:?}")),
    };

    let value = parse_literal(p)?;

    Ok(Expr::Binary { column, op, value })
}

fn parse_literal(p: &mut Parser) -> Result<LiteralValue, String> {
    match p.advance() {
        Token::StringLit(s) => Ok(LiteralValue::String(s)),
        Token::Number(s) => {
            if s.contains('.') {
                s.parse::<f64>()
                    .map(LiteralValue::Float)
                    .map_err(|e| format!("invalid float: {e}"))
            } else {
                s.parse::<i64>()
                    .map(LiteralValue::Int)
                    .map_err(|e| format!("invalid integer: {e}"))
            }
        }
        Token::True => Ok(LiteralValue::Bool(true)),
        Token::False => Ok(LiteralValue::Bool(false)),
        other => Err(format!("expected literal, got {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_all() {
        let stmt = parse("SELECT * FROM my_table").unwrap();
        assert!(stmt.select_all);
        assert_eq!(stmt.table, "my_table");
        assert!(stmt.schema.is_none());
        assert!(stmt.where_clause.is_none());
    }

    #[test]
    fn test_select_columns() {
        let stmt = parse("SELECT col1, col2 FROM ns.tbl").unwrap();
        assert!(!stmt.select_all);
        assert_eq!(stmt.columns, vec!["col1", "col2"]);
        assert_eq!(stmt.schema.as_deref(), Some("ns"));
        assert_eq!(stmt.table, "tbl");
    }

    #[test]
    fn test_where_eq() {
        let stmt = parse("SELECT * FROM t WHERE id = 42").unwrap();
        assert!(matches!(stmt.where_clause, Some(Expr::Binary { .. })));
    }

    #[test]
    fn test_where_string() {
        let stmt = parse("SELECT * FROM t WHERE name = 'alice'").unwrap();
        match stmt.where_clause {
            Some(Expr::Binary { value: LiteralValue::String(s), .. }) => assert_eq!(s, "alice"),
            _ => panic!("expected string literal"),
        }
    }

    #[test]
    fn test_where_and() {
        let stmt = parse("SELECT * FROM t WHERE a = 1 AND b > 2").unwrap();
        assert!(matches!(stmt.where_clause, Some(Expr::And(_, _))));
    }

    #[test]
    fn test_rejects_join() {
        assert!(parse("SELECT * FROM a JOIN b ON a.id = b.id").is_err());
    }
}
