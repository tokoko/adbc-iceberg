package sqlparser

import (
	"fmt"
	"strings"
	"unicode"
)

type tokenType int

const (
	tokEOF tokenType = iota
	tokIdent
	tokString
	tokNumber
	tokFloat
	tokComma
	tokDot
	tokStar
	tokLParen
	tokRParen
	tokEq
	tokNeq
	tokLt
	tokLte
	tokGt
	tokGte

	// Keywords
	tokSelect
	tokFrom
	tokWhere
	tokAnd
	tokOr
	tokNot
	tokIn
	tokIs
	tokNull
	tokBetween
	tokTrue
	tokFalse
)

type token struct {
	typ tokenType
	val string
}

type lexer struct {
	input  string
	pos    int
	tokens []token
}

var keywords = map[string]tokenType{
	"SELECT":  tokSelect,
	"FROM":    tokFrom,
	"WHERE":   tokWhere,
	"AND":     tokAnd,
	"OR":      tokOr,
	"NOT":     tokNot,
	"IN":      tokIn,
	"IS":      tokIs,
	"NULL":    tokNull,
	"BETWEEN": tokBetween,
	"TRUE":    tokTrue,
	"FALSE":   tokFalse,
}

func lex(input string) ([]token, error) {
	l := &lexer{input: input}
	if err := l.scan(); err != nil {
		return nil, err
	}
	return l.tokens, nil
}

func (l *lexer) scan() error {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]

		// Skip whitespace.
		if unicode.IsSpace(rune(ch)) {
			l.pos++
			continue
		}

		switch {
		case ch == ',':
			l.emit(tokComma, ",")
		case ch == '.':
			l.emit(tokDot, ".")
		case ch == '*':
			l.emit(tokStar, "*")
		case ch == '(':
			l.emit(tokLParen, "(")
		case ch == ')':
			l.emit(tokRParen, ")")
		case ch == '=':
			l.emit(tokEq, "=")
		case ch == '<':
			if l.peek() == '=' {
				l.pos++
				l.emit(tokLte, "<=")
			} else if l.peek() == '>' {
				l.pos++
				l.emit(tokNeq, "<>")
			} else {
				l.emit(tokLt, "<")
			}
		case ch == '>':
			if l.peek() == '=' {
				l.pos++
				l.emit(tokGte, ">=")
			} else {
				l.emit(tokGt, ">")
			}
		case ch == '!':
			if l.peek() == '=' {
				l.pos++
				l.emit(tokNeq, "!=")
			} else {
				return fmt.Errorf("unexpected character '!' at position %d", l.pos)
			}
		case ch == '\'':
			s, err := l.readString()
			if err != nil {
				return err
			}
			l.tokens = append(l.tokens, token{tokString, s})
			continue // readString already advances pos
		case ch == '-' && l.pos+1 < len(l.input) && isDigit(l.input[l.pos+1]):
			tok, err := l.readNumber()
			if err != nil {
				return err
			}
			l.tokens = append(l.tokens, tok)
			continue
		case isDigit(ch):
			tok, err := l.readNumber()
			if err != nil {
				return err
			}
			l.tokens = append(l.tokens, tok)
			continue
		case isIdentStart(ch):
			ident := l.readIdent()
			upper := strings.ToUpper(ident)
			if kwType, ok := keywords[upper]; ok {
				l.tokens = append(l.tokens, token{kwType, upper})
			} else {
				l.tokens = append(l.tokens, token{tokIdent, ident})
			}
			continue
		default:
			return fmt.Errorf("unexpected character '%c' at position %d", ch, l.pos)
		}
	}

	l.tokens = append(l.tokens, token{tokEOF, ""})
	return nil
}

func (l *lexer) emit(typ tokenType, val string) {
	l.tokens = append(l.tokens, token{typ, val})
	l.pos++
}

func (l *lexer) peek() byte {
	if l.pos+1 < len(l.input) {
		return l.input[l.pos+1]
	}
	return 0
}

func (l *lexer) readString() (string, error) {
	l.pos++ // skip opening quote
	start := l.pos
	for l.pos < len(l.input) {
		if l.input[l.pos] == '\'' {
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'' {
				l.pos += 2 // escaped quote
				continue
			}
			s := strings.ReplaceAll(l.input[start:l.pos], "''", "'")
			l.pos++ // skip closing quote
			return s, nil
		}
		l.pos++
	}
	return "", fmt.Errorf("unterminated string literal")
}

func (l *lexer) readNumber() (token, error) {
	start := l.pos
	if l.input[l.pos] == '-' {
		l.pos++
	}
	isFloat := false
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '.' && !isFloat {
			isFloat = true
			l.pos++
			continue
		}
		if !isDigit(ch) {
			break
		}
		l.pos++
	}
	val := l.input[start:l.pos]
	if isFloat {
		return token{tokFloat, val}, nil
	}
	return token{tokNumber, val}, nil
}

func (l *lexer) readIdent() string {
	start := l.pos
	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.pos++
	}
	return l.input[start:l.pos]
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(ch byte) bool {
	return isIdentStart(ch) || isDigit(ch)
}
