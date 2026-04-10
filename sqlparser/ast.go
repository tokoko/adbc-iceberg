// Package sqlparser implements a restricted SQL parser that only accepts
// SELECT <columns> FROM <table> [WHERE <filter>] statements.
package sqlparser

// SelectStatement represents a parsed SELECT query.
type SelectStatement struct {
	SelectAll bool     // true if SELECT *
	Columns   []string // column names (empty if SelectAll)
	Schema    string   // optional namespace/schema prefix
	Table     string   // table name
	Where     Expr     // optional WHERE clause (nil if none)
}

// Expr is the interface for all expression nodes in the WHERE clause.
type Expr interface {
	exprNode()
}

// BinaryExpr represents a comparison: column <op> literal.
type BinaryExpr struct {
	Column string
	Op     BinaryOp
	Value  LiteralValue
}

// AndExpr represents a logical AND of two expressions.
type AndExpr struct {
	Left, Right Expr
}

// OrExpr represents a logical OR of two expressions.
type OrExpr struct {
	Left, Right Expr
}

// NotExpr represents a logical NOT of an expression.
type NotExpr struct {
	Expr Expr
}

// IsNullExpr represents column IS NULL.
type IsNullExpr struct {
	Column string
}

// IsNotNullExpr represents column IS NOT NULL.
type IsNotNullExpr struct {
	Column string
}

// InExpr represents column IN (val1, val2, ...).
type InExpr struct {
	Column string
	Values []LiteralValue
}

// NotInExpr represents column NOT IN (val1, val2, ...).
type NotInExpr struct {
	Column string
	Values []LiteralValue
}

// BetweenExpr represents column BETWEEN low AND high.
type BetweenExpr struct {
	Column string
	Low    LiteralValue
	High   LiteralValue
}

func (*BinaryExpr) exprNode()    {}
func (*AndExpr) exprNode()       {}
func (*OrExpr) exprNode()        {}
func (*NotExpr) exprNode()       {}
func (*IsNullExpr) exprNode()    {}
func (*IsNotNullExpr) exprNode() {}
func (*InExpr) exprNode()        {}
func (*NotInExpr) exprNode()     {}
func (*BetweenExpr) exprNode()   {}

// BinaryOp represents a comparison operator.
type BinaryOp int

const (
	OpEq  BinaryOp = iota // =
	OpNeq                  // != or <>
	OpLt                   // <
	OpLte                  // <=
	OpGt                   // >
	OpGte                  // >=
)

// LiteralValue is the interface for literal values in expressions.
type LiteralValue interface {
	literalValue()
}

// StringLiteral represents a quoted string value.
type StringLiteral string

// IntLiteral represents an integer value.
type IntLiteral int64

// FloatLiteral represents a floating-point value.
type FloatLiteral float64

// BoolLiteral represents a boolean value.
type BoolLiteral bool

func (StringLiteral) literalValue() {}
func (IntLiteral) literalValue()    {}
func (FloatLiteral) literalValue()  {}
func (BoolLiteral) literalValue()   {}
