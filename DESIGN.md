# ADBC Iceberg Driver — Interface Mapping

## Overview

This document maps ADBC interface methods to Iceberg operations via `iceberg-go`.
The driver scope for v1 is: **REST catalog, Parquet-only, read-heavy, with partitioned execution**.

---

## Driver

| ADBC Method | Iceberg Mapping | Notes |
|---|---|---|
| `NewDatabase(opts)` | Construct driver database with config | Passes options like catalog URI, auth tokens |

**DriverInfo registration:**

| Info Code | Value |
|---|---|
| `InfoVendorName` | `"Apache Iceberg"` |
| `InfoDriverName` | `"ADBC Iceberg Driver"` |
| `InfoDriverVersion` | `"0.1.0"` |
| `InfoDriverArrowVersion` | Arrow Go version |

---

## Database

| ADBC Method | Iceberg Mapping | Notes |
|---|---|---|
| `SetOption(key, val)` | Store config for catalog construction | See option keys below |
| `Open(ctx)` | `rest.NewCatalog(ctx, name, uri, opts...)` | Creates REST catalog client, returns Connection |
| `Close()` | Release catalog client | |

**Database option keys:**

| Option Key | Description | Maps To |
|---|---|---|
| `uri` (standard) | REST catalog URI | `rest.NewCatalog` uri param |
| `adbc.iceberg.catalog.name` | Catalog name | `rest.NewCatalog` name param |
| `adbc.iceberg.auth.token` | Static bearer token | `rest.WithOAuthToken(token)` |
| `adbc.iceberg.auth.credential` | OAuth2 client credentials (`clientID:clientSecret`) | `rest.WithCredential(cred)` |
| `adbc.iceberg.auth.scope` | OAuth2 scope | `rest.WithScope(scope)` |
| `adbc.iceberg.auth.uri` | Custom token endpoint | `rest.WithAuthURI(uri)` |
| `username` (standard) | Not used in v1 | Error or ignore |
| `password` (standard) | Not used in v1 | Error or ignore |

---

## Connection

### Catalog Enumeration (via `DbObjectsEnumerator`)

| ADBC Method | Iceberg Mapping | Notes |
|---|---|---|
| `GetCatalogs(ctx, filter)` | Return configured catalog name | Iceberg REST catalog is a single catalog |
| `GetDBSchemasForCatalog(ctx, catalog, filter)` | `catalog.ListNamespaces(ctx, parent)` | Iceberg namespaces map to DB schemas |
| `GetTablesForDBSchema(ctx, catalog, schema, tableFilter, colFilter, includeCols)` | `catalog.ListTables(ctx, namespace)` + optionally `catalog.LoadTable` for columns | If `includeCols`, load each table's schema and convert with `table.SchemaToArrowSchema` |

### Other Connection Methods

| ADBC Method | Iceberg Mapping | Notes |
|---|---|---|
| `GetTableSchema(ctx, catalog, schema, table)` | `catalog.LoadTable(ctx, ident)` then `tbl.Schema()` then `table.SchemaToArrowSchema(...)` | Direct schema lookup |
| `GetTableTypes(ctx)` | Return `["TABLE"]` | Iceberg only has tables (no views in iceberg-go yet) |
| `GetInfo(ctx, codes)` | Return registered driver info | Static metadata |
| `NewStatement()` | Construct driver statement | |
| `ReadPartition(ctx, serialized)` | Deserialize `FileScanTask` descriptor, execute scan via `Scan.ReadTasks` | Core of partitioned execution |
| `Close()` | Release resources | |

### Connection Options (via `CurrentNamespacer`)

| ADBC Method | Iceberg Mapping | Notes |
|---|---|---|
| `SetCurrentCatalog(name)` | Validate catalog name matches | Single catalog, so mostly a no-op or error if mismatched |
| `GetCurrentCatalog()` | Return catalog name | |
| `SetCurrentDbSchema(name)` | Store as default namespace | Used as default for unqualified table references |
| `GetCurrentDbSchema()` | Return current namespace | |

### Not Implemented (v1)

| ADBC Method | Reason |
|---|---|
| `Commit(ctx)` / `Rollback(ctx)` | Iceberg is not transactional in the ADBC sense |
| `GetStatistics` | Not applicable for v1 |
| `AutocommitSetter` | Not applicable |

---

## Statement

### Query Execution

| ADBC Method | Iceberg Mapping | Notes |
|---|---|---|
| `SetSqlQuery(query)` | Parse restricted SQL: `SELECT <cols> FROM <table> [WHERE <filter>]` | Extract projection, table reference, filter expression |
| `ExecuteQuery(ctx)` | Full pipeline: resolve table -> build scan -> plan files -> read Arrow batches | Returns `array.RecordReader` streaming results |
| `ExecutePartitions(ctx)` | Resolve table -> build scan -> `scan.PlanFiles(ctx)` -> serialize each `FileScanTask` as partition descriptor | Returns `Partitions` + schema; engine calls `ReadPartition` per descriptor |
| `ExecuteSchema(ctx)` | Resolve table -> `table.SchemaToArrowSchema(tbl.Schema(), ...)` | Schema without execution |
| `ExecuteUpdate(ctx)` | Not supported (v1 is read-only) | Return error |

### SQL Parsing -> Iceberg Scan

The restricted SQL parser extracts three things:

```
SELECT col1, col2 FROM [namespace.]table WHERE col3 = 'value' AND col4 > 10
       ^^^^^^^^^^      ^^^^^^^^^^^^^^^^^       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
       projection      table identifier        filter expression
```

| SQL Component | Iceberg Mapping |
|---|---|
| Column list (`*` or named) | `table.WithSelectedFields(fields...)` |
| Table name | `catalog.LoadTable(ctx, ident)` — namespace from connection default or qualified name |
| WHERE clause | Parse to `iceberg.BooleanExpression` tree using `iceberg.EqualTo`, `iceberg.GreaterThan`, `iceberg.And`, etc. -> `table.WithRowFilter(expr)` |

**Supported WHERE operators (v1):**

| SQL | Iceberg Expression |
|---|---|
| `=` | `iceberg.EqualTo(ref, lit)` |
| `!=` / `<>` | `iceberg.NotEqualTo(ref, lit)` |
| `<` | `iceberg.LessThan(ref, lit)` |
| `<=` | `iceberg.LessThanEqual(ref, lit)` |
| `>` | `iceberg.GreaterThan(ref, lit)` |
| `>=` | `iceberg.GreaterThanEqual(ref, lit)` |
| `IN (...)` | `iceberg.In(ref, lits...)` |
| `NOT IN (...)` | `iceberg.NotIn(ref, lits...)` |
| `IS NULL` | `iceberg.IsNull(ref)` |
| `IS NOT NULL` | `iceberg.NotNull(ref)` |
| `AND` | `iceberg.And(left, right)` |
| `OR` | `iceberg.Or(left, right)` |
| `NOT` | `iceberg.Not(expr)` |
| `BETWEEN` | `iceberg.And(iceberg.GreaterThanEqual(...), iceberg.LessThanEqual(...))` |
| `LIKE` | `iceberg.StartsWith(ref, prefix)` — only for `prefix%` patterns; reject complex patterns |

### Partitioned Execution Detail

```
Engine                          Driver
  |                               |
  |-- ExecutePartitions(ctx) ---->|
  |                               |-- catalog.LoadTable(ctx, ident)
  |                               |-- tbl.Scan(opts...).PlanFiles(ctx)
  |                               |-- for each FileScanTask:
  |                               |     serialize to []byte descriptor
  |                               |     (file path, byte range, snapshot ID,
  |                               |      projected fields, pushed filters)
  |<-- (schema, partitions, n) ---|
  |                               |
  | (engine distributes descriptors to workers)
  |                               |
  |-- ReadPartition(ctx, desc) -->|
  |                               |-- deserialize descriptor
  |                               |-- tbl.Scan(snapshotID, fields, filter)
  |                               |     .ReadTasks(ctx, []FileScanTask{task})
  |<-- RecordReader --------------|
```

**Partition descriptor format (serialized bytes):**

```
{
  "catalog_uri":    string,     // for descriptor portability
  "table_ident":    string,     // namespace.table
  "snapshot_id":    int64,      // pinned snapshot
  "file_path":      string,     // data file location
  "file_format":    string,     // PARQUET
  "start":          int64,      // byte offset
  "length":         int64,      // byte length
  "projected_fields": []string, // column projection
  "filter":         bytes,      // serialized iceberg expression (optional)
  "delete_files":   []string,   // associated delete file paths
}
```

### Substrait (Optional Fast Path)

| ADBC Method | Iceberg Mapping | Notes |
|---|---|---|
| `SetSubstraitPlan(plan)` | Deserialize Substrait ReadRel -> extract table, projection, filter | Optional; not required for v1 |

### Bulk Ingestion (Future)

| ADBC Method | Iceberg Mapping | Notes |
|---|---|---|
| `SetOption("adbc.ingest.target_table", name)` | Target table for write | Future: map to Iceberg append/overwrite |
| `Bind(ctx, batch)` / `BindStream(ctx, reader)` | Arrow data to write | Future: write Parquet files + commit to catalog |
| `ExecuteUpdate(ctx)` with ingest options | `catalog.CommitTable` with append operation | Future |

### Statement Options

| Option Key | Description |
|---|---|
| `adbc.iceberg.snapshot_id` | Scan a specific snapshot (time travel) |
| `adbc.iceberg.as_of_timestamp` | Scan as of timestamp (ms since epoch) |
| `adbc.iceberg.branch` | Scan a named branch |
| `adbc.iceberg.start_snapshot_id` | Incremental scan: start snapshot (exclusive) |
| `adbc.iceberg.batch_size` | Arrow batch size (default: 131072) |

---

## Incremental Reads

Incremental reads between two snapshots use a combination of statement options and partitioned execution:

1. Engine sets `adbc.iceberg.start_snapshot_id` and optionally `adbc.iceberg.snapshot_id` (end)
2. Driver walks snapshot parent chain from end to start
3. For each intermediate snapshot, reads manifests and collects ADDED data files
4. Returns these as partition descriptors via `ExecutePartitions`
5. Engine reads each partition normally via `ReadPartition`

This is custom logic since iceberg-go has no built-in incremental scan API.

---

## Error Handling

| Scenario | ADBC Error Code |
|---|---|
| Unsupported SQL syntax | `adbc.StatusInvalidArgument` |
| Table not found | `adbc.StatusNotFound` |
| Catalog auth failure | `adbc.StatusUnauthenticated` |
| Unsupported operation (writes, transactions) | `adbc.StatusNotImplemented` |
| File read failure (S3/GCS/local) | `adbc.StatusIO` |
| Invalid option key | `adbc.StatusInvalidArgument` |

---

## Dependency Stack

```
adbc-iceberg (this driver)
├── github.com/apache/arrow-adbc/go/adbc           # ADBC interfaces + driverbase
├── github.com/apache/iceberg-go                    # catalog, schema, expressions
├── github.com/apache/iceberg-go/catalog/rest       # REST catalog client
├── github.com/apache/iceberg-go/table              # scan planning, Arrow conversion
├── github.com/apache/arrow-go/v18                  # Arrow types, memory, record batches
└── (SQL parser - TBD: hand-rolled or lightweight library)
```
