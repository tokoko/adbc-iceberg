package sqlparser

import (
	"testing"
)

func TestParseSelectAll(t *testing.T) {
	stmt, err := Parse("SELECT * FROM my_table")
	if err != nil {
		t.Fatal(err)
	}
	if !stmt.SelectAll {
		t.Error("expected SelectAll")
	}
	if stmt.Table != "my_table" {
		t.Errorf("expected table 'my_table', got %q", stmt.Table)
	}
	if stmt.Schema != "" {
		t.Errorf("expected no schema, got %q", stmt.Schema)
	}
	if stmt.Where != nil {
		t.Error("expected no WHERE clause")
	}
}

func TestParseSelectColumns(t *testing.T) {
	stmt, err := Parse("SELECT col1, col2, col3 FROM ns.tbl")
	if err != nil {
		t.Fatal(err)
	}
	if stmt.SelectAll {
		t.Error("expected specific columns, not SelectAll")
	}
	if len(stmt.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(stmt.Columns))
	}
	if stmt.Schema != "ns" {
		t.Errorf("expected schema 'ns', got %q", stmt.Schema)
	}
	if stmt.Table != "tbl" {
		t.Errorf("expected table 'tbl', got %q", stmt.Table)
	}
}

func TestParseWhereEq(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE id = 42")
	if err != nil {
		t.Fatal(err)
	}
	bin, ok := stmt.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", stmt.Where)
	}
	if bin.Column != "id" {
		t.Errorf("expected column 'id', got %q", bin.Column)
	}
	if bin.Op != OpEq {
		t.Errorf("expected OpEq, got %v", bin.Op)
	}
	if val, ok := bin.Value.(IntLiteral); !ok || int64(val) != 42 {
		t.Errorf("expected IntLiteral(42), got %v", bin.Value)
	}
}

func TestParseWhereString(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE name = 'hello'")
	if err != nil {
		t.Fatal(err)
	}
	bin := stmt.Where.(*BinaryExpr)
	if val, ok := bin.Value.(StringLiteral); !ok || string(val) != "hello" {
		t.Errorf("expected StringLiteral('hello'), got %v", bin.Value)
	}
}

func TestParseWhereAnd(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE a = 1 AND b > 2")
	if err != nil {
		t.Fatal(err)
	}
	and, ok := stmt.Where.(*AndExpr)
	if !ok {
		t.Fatalf("expected AndExpr, got %T", stmt.Where)
	}
	left := and.Left.(*BinaryExpr)
	right := and.Right.(*BinaryExpr)
	if left.Column != "a" || left.Op != OpEq {
		t.Errorf("unexpected left: %v", left)
	}
	if right.Column != "b" || right.Op != OpGt {
		t.Errorf("unexpected right: %v", right)
	}
}

func TestParseWhereOr(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE a = 1 OR b = 2")
	if err != nil {
		t.Fatal(err)
	}
	_, ok := stmt.Where.(*OrExpr)
	if !ok {
		t.Fatalf("expected OrExpr, got %T", stmt.Where)
	}
}

func TestParseWhereIsNull(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE x IS NULL")
	if err != nil {
		t.Fatal(err)
	}
	isn, ok := stmt.Where.(*IsNullExpr)
	if !ok {
		t.Fatalf("expected IsNullExpr, got %T", stmt.Where)
	}
	if isn.Column != "x" {
		t.Errorf("expected column 'x', got %q", isn.Column)
	}
}

func TestParseWhereIsNotNull(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE x IS NOT NULL")
	if err != nil {
		t.Fatal(err)
	}
	_, ok := stmt.Where.(*IsNotNullExpr)
	if !ok {
		t.Fatalf("expected IsNotNullExpr, got %T", stmt.Where)
	}
}

func TestParseWhereIn(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE status IN ('a', 'b', 'c')")
	if err != nil {
		t.Fatal(err)
	}
	in, ok := stmt.Where.(*InExpr)
	if !ok {
		t.Fatalf("expected InExpr, got %T", stmt.Where)
	}
	if in.Column != "status" {
		t.Errorf("expected column 'status', got %q", in.Column)
	}
	if len(in.Values) != 3 {
		t.Errorf("expected 3 values, got %d", len(in.Values))
	}
}

func TestParseWhereNotIn(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE x NOT IN (1, 2)")
	if err != nil {
		t.Fatal(err)
	}
	_, ok := stmt.Where.(*NotInExpr)
	if !ok {
		t.Fatalf("expected NotInExpr, got %T", stmt.Where)
	}
}

func TestParseWhereBetween(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE age BETWEEN 18 AND 65")
	if err != nil {
		t.Fatal(err)
	}
	bet, ok := stmt.Where.(*BetweenExpr)
	if !ok {
		t.Fatalf("expected BetweenExpr, got %T", stmt.Where)
	}
	if bet.Column != "age" {
		t.Errorf("expected column 'age', got %q", bet.Column)
	}
}

func TestParseWhereNot(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE NOT a = 1")
	if err != nil {
		t.Fatal(err)
	}
	not, ok := stmt.Where.(*NotExpr)
	if !ok {
		t.Fatalf("expected NotExpr, got %T", stmt.Where)
	}
	_, ok = not.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr inside NOT, got %T", not.Expr)
	}
}

func TestParseWhereParens(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE (a = 1 OR b = 2) AND c = 3")
	if err != nil {
		t.Fatal(err)
	}
	and, ok := stmt.Where.(*AndExpr)
	if !ok {
		t.Fatalf("expected AndExpr, got %T", stmt.Where)
	}
	_, ok = and.Left.(*OrExpr)
	if !ok {
		t.Fatalf("expected OrExpr on left, got %T", and.Left)
	}
}

func TestParseWhereFloat(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE price > 9.99")
	if err != nil {
		t.Fatal(err)
	}
	bin := stmt.Where.(*BinaryExpr)
	if val, ok := bin.Value.(FloatLiteral); !ok || float64(val) != 9.99 {
		t.Errorf("expected FloatLiteral(9.99), got %v", bin.Value)
	}
}

func TestParseWhereBool(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE active = true")
	if err != nil {
		t.Fatal(err)
	}
	bin := stmt.Where.(*BinaryExpr)
	if val, ok := bin.Value.(BoolLiteral); !ok || !bool(val) {
		t.Errorf("expected BoolLiteral(true), got %v", bin.Value)
	}
}

func TestParseNegativeNumber(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE x > -5")
	if err != nil {
		t.Fatal(err)
	}
	bin := stmt.Where.(*BinaryExpr)
	if val, ok := bin.Value.(IntLiteral); !ok || int64(val) != -5 {
		t.Errorf("expected IntLiteral(-5), got %v", bin.Value)
	}
}

func TestParseEscapedString(t *testing.T) {
	stmt, err := Parse("SELECT * FROM tbl WHERE name = 'it''s'")
	if err != nil {
		t.Fatal(err)
	}
	bin := stmt.Where.(*BinaryExpr)
	if val, ok := bin.Value.(StringLiteral); !ok || string(val) != "it's" {
		t.Errorf("expected StringLiteral(\"it's\"), got %v", bin.Value)
	}
}

func TestParseOperators(t *testing.T) {
	ops := []struct {
		sql string
		op  BinaryOp
	}{
		{"SELECT * FROM t WHERE a != 1", OpNeq},
		{"SELECT * FROM t WHERE a <> 1", OpNeq},
		{"SELECT * FROM t WHERE a < 1", OpLt},
		{"SELECT * FROM t WHERE a <= 1", OpLte},
		{"SELECT * FROM t WHERE a > 1", OpGt},
		{"SELECT * FROM t WHERE a >= 1", OpGte},
	}
	for _, tc := range ops {
		stmt, err := Parse(tc.sql)
		if err != nil {
			t.Errorf("Parse(%q) failed: %v", tc.sql, err)
			continue
		}
		bin := stmt.Where.(*BinaryExpr)
		if bin.Op != tc.op {
			t.Errorf("Parse(%q): expected op %v, got %v", tc.sql, tc.op, bin.Op)
		}
	}
}

func TestParseRejectsJoin(t *testing.T) {
	_, err := Parse("SELECT * FROM a JOIN b ON a.id = b.id")
	if err == nil {
		t.Error("expected error for JOIN query")
	}
}

func TestParseRejectsSubquery(t *testing.T) {
	_, err := Parse("SELECT * FROM (SELECT * FROM t)")
	if err == nil {
		t.Error("expected error for subquery")
	}
}

func TestParseRejectsInsert(t *testing.T) {
	_, err := Parse("INSERT INTO t VALUES (1)")
	if err == nil {
		t.Error("expected error for INSERT")
	}
}

func TestParseCaseInsensitiveKeywords(t *testing.T) {
	stmt, err := Parse("select * from tbl where a = 1")
	if err != nil {
		t.Fatal(err)
	}
	if stmt.Table != "tbl" {
		t.Errorf("expected table 'tbl', got %q", stmt.Table)
	}
}
