package iceberg

import (
	"context"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	iceTable "github.com/apache/iceberg-go/table"
)

type connection struct {
	driverbase.ConnectionImplBase

	db          *database
	cat         catalog.Catalog
	curCatalog  string
	curDbSchema string
}

// Verify interface compliance.
var (
	_ driverbase.DbObjectsEnumerator = (*connection)(nil)
	_ driverbase.CurrentNamespacer   = (*connection)(nil)
	_ driverbase.TableTypeLister     = (*connection)(nil)
)

func newConnection(db *database, cat catalog.Catalog) *connection {
	return &connection{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&db.DatabaseImplBase),
		db:                 db,
		cat:                cat,
		curCatalog:         db.catalogName,
	}
}

// --- CurrentNamespacer ---

func (c *connection) GetCurrentCatalog() (string, error) {
	return c.curCatalog, nil
}

func (c *connection) SetCurrentCatalog(name string) error {
	if name != c.db.catalogName {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "catalog name does not match configured catalog",
		}
	}
	c.curCatalog = name
	return nil
}

func (c *connection) GetCurrentDbSchema() (string, error) {
	return c.curDbSchema, nil
}

func (c *connection) SetCurrentDbSchema(name string) error {
	c.curDbSchema = name
	return nil
}

// --- TableTypeLister ---

func (c *connection) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE"}, nil
}

// --- DbObjectsEnumerator ---

func (c *connection) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	name := c.db.catalogName
	if catalogFilter != nil && !matchPattern(*catalogFilter, name) {
		return nil, nil
	}
	return []string{name}, nil
}

func (c *connection) GetDBSchemasForCatalog(ctx context.Context, catalogName string, schemaFilter *string) ([]string, error) {
	namespaces, err := c.cat.ListNamespaces(ctx, catalog.ToIdentifier())
	if err != nil {
		return nil, err
	}

	var result []string
	for _, ns := range namespaces {
		name := strings.Join(ns, ".")
		if schemaFilter != nil && !matchPattern(*schemaFilter, name) {
			continue
		}
		result = append(result, name)
	}
	return result, nil
}

func (c *connection) GetTablesForDBSchema(ctx context.Context, catalogName, dbSchema string, tableFilter, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	ns := catalog.ToIdentifier(dbSchema)

	var tables []driverbase.TableInfo
	for ident, err := range c.cat.ListTables(ctx, ns) {
		if err != nil {
			return nil, err
		}

		name := ident[len(ident)-1]
		if tableFilter != nil && !matchPattern(*tableFilter, name) {
			continue
		}

		info := driverbase.TableInfo{
			TableName: name,
			TableType: "TABLE",
		}

		if includeColumns {
			cols, err := c.getColumnsForTable(ctx, ident, columnFilter)
			if err != nil {
				return nil, err
			}
			info.TableColumns = cols
		}

		tables = append(tables, info)
	}
	return tables, nil
}

func (c *connection) getColumnsForTable(ctx context.Context, ident iceTable.Identifier, columnFilter *string) ([]driverbase.ColumnInfo, error) {
	tbl, err := c.cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}

	arrowSchema, err := iceTable.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	if err != nil {
		return nil, err
	}

	var cols []driverbase.ColumnInfo
	for i, field := range arrowSchema.Fields() {
		if columnFilter != nil && !matchPattern(*columnFilter, field.Name) {
			continue
		}
		pos := int32(i + 1)
		cols = append(cols, driverbase.ColumnInfo{
			ColumnName:      field.Name,
			OrdinalPosition: &pos,
		})
	}
	return cols, nil
}

// GetTableSchema returns the Arrow schema for a specific table.
func (c *connection) GetTableSchema(ctx context.Context, catalogPtr, dbSchemaPtr *string, tableName string) (*arrow.Schema, error) {
	ident := c.resolveTableIdent(dbSchemaPtr, tableName)

	tbl, err := c.cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
			Msg:  err.Error(),
		}
	}

	return iceTable.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
}

// NewStatement creates a new statement for query execution.
func (c *connection) NewStatement() (adbc.Statement, error) {
	return newStatement(c)
}

// ReadPartition reads a partition previously returned by ExecutePartitions.
func (c *connection) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	desc, err := deserializePartitionDescriptor(serializedPartition)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  err.Error(),
		}
	}

	return c.readPartitionDescriptor(ctx, desc)
}

func (c *connection) Close() error {
	return nil
}

// resolveTableIdent builds an Iceberg Identifier from optional schema and table name.
func (c *connection) resolveTableIdent(dbSchemaPtr *string, tableName string) iceTable.Identifier {
	switch {
	case dbSchemaPtr != nil:
		return catalog.ToIdentifier(*dbSchemaPtr + "." + tableName)
	case c.curDbSchema != "":
		return catalog.ToIdentifier(c.curDbSchema + "." + tableName)
	default:
		return catalog.ToIdentifier(tableName)
	}
}

// matchPattern does simple SQL LIKE-style matching with % wildcards.
func matchPattern(pattern, value string) bool {
	if pattern == "%" || pattern == "" {
		return true
	}
	if strings.HasSuffix(pattern, "%") && !strings.Contains(pattern[:len(pattern)-1], "%") {
		return strings.HasPrefix(value, pattern[:len(pattern)-1])
	}
	if strings.HasPrefix(pattern, "%") && !strings.Contains(pattern[1:], "%") {
		return strings.HasSuffix(value, pattern[1:])
	}
	if !strings.Contains(pattern, "%") {
		return value == pattern
	}
	return value == pattern
}
