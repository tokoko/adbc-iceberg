package sqlparser

import (
	"fmt"
	"strconv"
)

// Parse parses a restricted SQL statement of the form:
//
//	SELECT <columns> FROM [schema.]<table> [WHERE <filter>]
func Parse(sql string) (*SelectStatement, error) {
	tokens, err := lex(sql)
	if err != nil {
		return nil, err
	}
	p := &parser{tokens: tokens}
	return p.parse()
}

type parser struct {
	tokens []token
	pos    int
}

func (p *parser) parse() (*SelectStatement, error) {
	if err := p.expect(tokSelect); err != nil {
		return nil, err
	}

	stmt := &SelectStatement{}

	// Parse column list.
	if p.peek().typ == tokStar {
		p.advance()
		stmt.SelectAll = true
	} else {
		cols, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = cols
	}

	// Parse FROM clause.
	if err := p.expect(tokFrom); err != nil {
		return nil, err
	}

	schema, table, err := p.parseTableRef()
	if err != nil {
		return nil, err
	}
	stmt.Schema = schema
	stmt.Table = table

	// Parse optional WHERE clause.
	if p.peek().typ == tokWhere {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	if p.peek().typ != tokEOF {
		return nil, fmt.Errorf("unexpected token %q after statement", p.peek().val)
	}

	return stmt, nil
}

func (p *parser) parseColumnList() ([]string, error) {
	var cols []string
	for {
		tok := p.advance()
		if tok.typ != tokIdent {
			return nil, fmt.Errorf("expected column name, got %q", tok.val)
		}
		cols = append(cols, tok.val)
		if p.peek().typ != tokComma {
			break
		}
		p.advance() // consume comma
	}
	return cols, nil
}

func (p *parser) parseTableRef() (schema, table string, err error) {
	tok := p.advance()
	if tok.typ != tokIdent {
		return "", "", fmt.Errorf("expected table name, got %q", tok.val)
	}
	name := tok.val

	if p.peek().typ == tokDot {
		p.advance() // consume dot
		tok2 := p.advance()
		if tok2.typ != tokIdent {
			return "", "", fmt.Errorf("expected table name after dot, got %q", tok2.val)
		}
		return name, tok2.val, nil
	}
	return "", name, nil
}

// parseExpr parses an expression with OR as the lowest precedence.
func (p *parser) parseExpr() (Expr, error) {
	return p.parseOr()
}

func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().typ == tokOr {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &OrExpr{Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.peek().typ == tokAnd {
		p.advance()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &AndExpr{Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseUnary() (Expr, error) {
	if p.peek().typ == tokNot {
		p.advance()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Expr: expr}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (Expr, error) {
	// Parenthesized expression.
	if p.peek().typ == tokLParen {
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokRParen); err != nil {
			return nil, err
		}
		return expr, nil
	}

	// Must be: column <op> value | column IS [NOT] NULL | column [NOT] IN (...) | column BETWEEN ...
	tok := p.advance()
	if tok.typ != tokIdent {
		return nil, fmt.Errorf("expected column name, got %q", tok.val)
	}
	column := tok.val

	next := p.peek()

	// IS [NOT] NULL
	if next.typ == tokIs {
		p.advance()
		if p.peek().typ == tokNot {
			p.advance()
			if err := p.expect(tokNull); err != nil {
				return nil, fmt.Errorf("expected NULL after IS NOT")
			}
			return &IsNotNullExpr{Column: column}, nil
		}
		if err := p.expect(tokNull); err != nil {
			return nil, fmt.Errorf("expected NULL after IS")
		}
		return &IsNullExpr{Column: column}, nil
	}

	// NOT IN
	if next.typ == tokNot {
		p.advance()
		if p.peek().typ != tokIn {
			return nil, fmt.Errorf("expected IN after NOT")
		}
		p.advance()
		vals, err := p.parseLiteralList()
		if err != nil {
			return nil, err
		}
		return &NotInExpr{Column: column, Values: vals}, nil
	}

	// IN
	if next.typ == tokIn {
		p.advance()
		vals, err := p.parseLiteralList()
		if err != nil {
			return nil, err
		}
		return &InExpr{Column: column, Values: vals}, nil
	}

	// BETWEEN
	if next.typ == tokBetween {
		p.advance()
		low, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		if err := p.expect(tokAnd); err != nil {
			return nil, fmt.Errorf("expected AND in BETWEEN expression")
		}
		high, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Column: column, Low: low, High: high}, nil
	}

	// Comparison operator.
	op, err := p.parseCompOp()
	if err != nil {
		return nil, err
	}

	val, err := p.parseLiteral()
	if err != nil {
		return nil, err
	}

	return &BinaryExpr{Column: column, Op: op, Value: val}, nil
}

func (p *parser) parseCompOp() (BinaryOp, error) {
	tok := p.advance()
	switch tok.typ {
	case tokEq:
		return OpEq, nil
	case tokNeq:
		return OpNeq, nil
	case tokLt:
		return OpLt, nil
	case tokLte:
		return OpLte, nil
	case tokGt:
		return OpGt, nil
	case tokGte:
		return OpGte, nil
	default:
		return 0, fmt.Errorf("expected comparison operator, got %q", tok.val)
	}
}

func (p *parser) parseLiteral() (LiteralValue, error) {
	tok := p.advance()
	switch tok.typ {
	case tokString:
		return StringLiteral(tok.val), nil
	case tokNumber:
		v, err := strconv.ParseInt(tok.val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q: %w", tok.val, err)
		}
		return IntLiteral(v), nil
	case tokFloat:
		v, err := strconv.ParseFloat(tok.val, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float %q: %w", tok.val, err)
		}
		return FloatLiteral(v), nil
	case tokTrue:
		return BoolLiteral(true), nil
	case tokFalse:
		return BoolLiteral(false), nil
	default:
		return nil, fmt.Errorf("expected literal value, got %q", tok.val)
	}
}

func (p *parser) parseLiteralList() ([]LiteralValue, error) {
	if err := p.expect(tokLParen); err != nil {
		return nil, err
	}

	var vals []LiteralValue
	for {
		val, err := p.parseLiteral()
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
		if p.peek().typ != tokComma {
			break
		}
		p.advance() // consume comma
	}

	if err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	return vals, nil
}

func (p *parser) peek() token {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return token{tokEOF, ""}
}

func (p *parser) advance() token {
	tok := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *parser) expect(typ tokenType) error {
	tok := p.advance()
	if tok.typ != typ {
		return fmt.Errorf("expected token type %d, got %q", typ, tok.val)
	}
	return nil
}
