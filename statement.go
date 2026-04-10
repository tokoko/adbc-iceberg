package iceberg

import (
	"context"
	"fmt"
	"strconv"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	iceberggo "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceTable "github.com/apache/iceberg-go/table"

	"github.com/tokoko/adbc-iceberg/sqlparser"
)

type statement struct {
	driverbase.StatementImplBase
	conn *connection

	query  string
	parsed *sqlparser.SelectStatement

	// Statement-level options for scan configuration.
	snapshotID      *int64
	asOfTimestamp   *int64
	branch          string
	startSnapshotID *int64
	batchSize       int
}

func (s *statement) Base() *driverbase.StatementImplBase {
	return &s.StatementImplBase
}

func newStatement(conn *connection) (adbc.Statement, error) {
	st := &statement{
		StatementImplBase: driverbase.NewStatementImplBase(&conn.ConnectionImplBase, conn.ConnectionImplBase.ErrorHelper),
		conn:              conn,
		batchSize:         131072,
	}
	return driverbase.NewStatement(st), nil
}

func (s *statement) SetSqlQuery(query string) error {
	parsed, err := sqlparser.Parse(query)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unsupported SQL: %v", err),
		}
	}
	s.query = query
	s.parsed = parsed
	return nil
}

func (s *statement) SetOption(key, value string) error {
	switch key {
	case OptionKeySnapshotID:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "invalid snapshot_id"}
		}
		s.snapshotID = &v
	case OptionKeyAsOfTimestamp:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "invalid as_of_timestamp"}
		}
		s.asOfTimestamp = &v
	case OptionKeyBranch:
		s.branch = value
	case OptionKeyStartSnapshotID:
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "invalid start_snapshot_id"}
		}
		s.startSnapshotID = &v
	case OptionKeyBatchSize:
		v, err := strconv.Atoi(value)
		if err != nil || v <= 0 {
			return adbc.Error{Code: adbc.StatusInvalidArgument, Msg: "invalid batch_size"}
		}
		s.batchSize = v
	default:
		return s.StatementImplBase.SetOption(key, value)
	}
	return nil
}

func (s *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.parsed == nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}
	return s.executeQuery(ctx)
}

func (s *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	if s.parsed == nil {
		return nil, adbc.Partitions{}, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}
	return s.executePartitions(ctx)
}

func (s *statement) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	if s.parsed == nil {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}

	tbl, err := s.resolveTable(ctx)
	if err != nil {
		return nil, err
	}
	return s.resolveSchema(tbl)
}

func (s *statement) Prepare(ctx context.Context) error {
	return nil
}

func (s *statement) SetSubstraitPlan(plan []byte) error {
	return adbc.Error{Code: adbc.StatusNotImplemented, Msg: "SetSubstraitPlan not supported"}
}

func (s *statement) Bind(ctx context.Context, values arrow.Record) error {
	return adbc.Error{Code: adbc.StatusNotImplemented, Msg: "Bind not supported"}
}

func (s *statement) BindStream(ctx context.Context, stream array.RecordReader) error {
	return adbc.Error{Code: adbc.StatusNotImplemented, Msg: "BindStream not supported"}
}

func (s *statement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{Code: adbc.StatusNotImplemented, Msg: "GetParameterSchema not supported"}
}

func (s *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	return -1, adbc.Error{Code: adbc.StatusNotImplemented, Msg: "ExecuteUpdate not supported"}
}

func (s *statement) Close() error {
	s.parsed = nil
	return nil
}

// resolveTable loads the Iceberg table referenced by the parsed query.
func (s *statement) resolveTable(ctx context.Context) (*iceTable.Table, error) {
	var ident iceTable.Identifier
	if s.parsed.Schema != "" {
		ident = catalog.ToIdentifier(s.parsed.Schema + "." + s.parsed.Table)
	} else {
		ident = s.conn.resolveTableIdent(nil, s.parsed.Table)
	}

	tbl, err := s.conn.cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  fmt.Sprintf("table not found: %v", err),
		}
	}
	return tbl, nil
}

// resolveSchema returns the Arrow schema for the resolved table with projection applied.
func (s *statement) resolveSchema(tbl *iceTable.Table) (*arrow.Schema, error) {
	if s.parsed.SelectAll {
		return iceTable.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	}

	projected, err := tbl.Schema().Select(true, s.parsed.Columns...)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("column projection failed: %v", err),
		}
	}
	return iceTable.SchemaToArrowSchema(projected, nil, false, false)
}

// buildScanOptions constructs the scan options from the parsed query and statement options.
func (s *statement) buildScanOptions() []iceTable.ScanOption {
	var opts []iceTable.ScanOption

	if s.parsed.SelectAll {
		opts = append(opts, iceTable.WithSelectedFields("*"))
	} else {
		opts = append(opts, iceTable.WithSelectedFields(s.parsed.Columns...))
	}

	if s.snapshotID != nil {
		opts = append(opts, iceTable.WithSnapshotID(*s.snapshotID))
	}
	if s.asOfTimestamp != nil {
		opts = append(opts, iceTable.WithSnapshotAsOf(*s.asOfTimestamp))
	}

	if s.parsed.Where != nil {
		if expr, err := convertWhereToIceberg(s.parsed.Where); err == nil {
			opts = append(opts, iceTable.WithRowFilter(expr))
		}
	}

	return opts
}

// tableIdent returns the fully qualified table identifier string.
func (s *statement) tableIdent() string {
	if s.parsed.Schema != "" {
		return s.parsed.Schema + "." + s.parsed.Table
	}
	if s.conn.curDbSchema != "" {
		return s.conn.curDbSchema + "." + s.parsed.Table
	}
	return s.parsed.Table
}

// projectedFields returns the list of projected column names.
func (s *statement) projectedFields() []string {
	if s.parsed.SelectAll {
		return nil
	}
	return s.parsed.Columns
}

// convertWhereToIceberg converts a parsed WHERE clause AST into an Iceberg BooleanExpression.
func convertWhereToIceberg(expr sqlparser.Expr) (iceberggo.BooleanExpression, error) {
	switch e := expr.(type) {
	case *sqlparser.BinaryExpr:
		return convertBinaryExpr(e)
	case *sqlparser.AndExpr:
		left, err := convertWhereToIceberg(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertWhereToIceberg(e.Right)
		if err != nil {
			return nil, err
		}
		return iceberggo.NewAnd(left, right), nil
	case *sqlparser.OrExpr:
		left, err := convertWhereToIceberg(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := convertWhereToIceberg(e.Right)
		if err != nil {
			return nil, err
		}
		return iceberggo.NewOr(left, right), nil
	case *sqlparser.NotExpr:
		inner, err := convertWhereToIceberg(e.Expr)
		if err != nil {
			return nil, err
		}
		return iceberggo.NewNot(inner), nil
	case *sqlparser.IsNullExpr:
		return iceberggo.IsNull(iceberggo.Reference(e.Column)), nil
	case *sqlparser.IsNotNullExpr:
		return iceberggo.NotNull(iceberggo.Reference(e.Column)), nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func convertBinaryExpr(e *sqlparser.BinaryExpr) (iceberggo.BooleanExpression, error) {
	ref := iceberggo.Reference(e.Column)

	switch val := e.Value.(type) {
	case sqlparser.StringLiteral:
		return applyOp(e.Op, ref, string(val))
	case sqlparser.IntLiteral:
		return applyOp(e.Op, ref, int64(val))
	case sqlparser.FloatLiteral:
		return applyOp(e.Op, ref, float64(val))
	case sqlparser.BoolLiteral:
		return applyOp(e.Op, ref, bool(val))
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", e.Value)
	}
}

func applyOp[T iceberggo.LiteralType](op sqlparser.BinaryOp, ref iceberggo.Reference, val T) (iceberggo.BooleanExpression, error) {
	switch op {
	case sqlparser.OpEq:
		return iceberggo.EqualTo(ref, val), nil
	case sqlparser.OpNeq:
		return iceberggo.NotEqualTo(ref, val), nil
	case sqlparser.OpLt:
		return iceberggo.LessThan(ref, val), nil
	case sqlparser.OpLte:
		return iceberggo.LessThanEqual(ref, val), nil
	case sqlparser.OpGt:
		return iceberggo.GreaterThan(ref, val), nil
	case sqlparser.OpGte:
		return iceberggo.GreaterThanEqual(ref, val), nil
	default:
		return nil, fmt.Errorf("unsupported operator: %v", op)
	}
}
