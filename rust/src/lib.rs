#![allow(refining_impl_trait)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::Schema;

use adbc_core::{
    Connection, Database, Driver, Optionable, Statement,
    constants::{ADBC_CONNECTION_OPTION_AUTOCOMMIT, ADBC_CONNECTION_OPTION_CURRENT_CATALOG, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA},
    error::{Error, Result, Status},
    options::{InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue},
    schemas::GET_TABLE_TYPES_SCHEMA,
};
use driverbase::InfoRegistry;
use driverbase::error::ErrorHelper as _;
use driverbase::get_objects::{ColumnInfo, GetObjectsImpl, TableAndColumnInfo, TableInfo};

use iceberg::expr::{Predicate, Reference};
use iceberg::io::StorageFactory;
use iceberg::scan::TableScan;
use iceberg::spec::Datum;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

mod sqlparser;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Option keys
// ---------------------------------------------------------------------------

/// Statement option: scan a specific snapshot.
const OPTION_SNAPSHOT_ID: &str = "adbc.iceberg.snapshot_id";
/// Statement option: scan the snapshot current as of a timestamp (ms since epoch).
const OPTION_AS_OF_TIMESTAMP: &str = "adbc.iceberg.as_of_timestamp";
/// Statement option: scan the head of a named branch or tag.
const OPTION_BRANCH: &str = "adbc.iceberg.branch";
/// Statement option: number of rows per Arrow batch.
const OPTION_BATCH_SIZE: &str = "adbc.iceberg.batch_size";

/// Namespace used for unqualified table names when no current db_schema is set.
const DEFAULT_NAMESPACE: &str = "default";
/// Iceberg only has one kind of table object.
const TABLE_TYPE: &str = "TABLE";

/// Partition descriptor — serialized as JSON in ExecutePartitions,
/// deserialized in ReadPartition. We store the query + task file path
/// so read_partition can re-plan and filter to the specific file.
#[derive(Serialize, Deserialize)]
struct PartitionDescriptor {
    namespace: String,
    table_name: String,
    /// The SQL query to re-execute for this partition.
    query: String,
    /// The data file path — used to filter planned tasks to just this one.
    data_file_path: String,
    /// Snapshot the partitions were planned against, so every partition reads
    /// the same table state even if the table is updated in between.
    #[serde(default)]
    snapshot_id: Option<i64>,
    #[serde(default)]
    batch_size: Option<usize>,
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct ErrorHelper {}

impl driverbase::error::ErrorHelper for ErrorHelper {
    const NAME: &'static str = "iceberg";
}

impl ErrorHelper {
    fn from_iceberg(err: iceberg::Error) -> driverbase::error::Error<ErrorHelper> {
        // The REST catalog reports HTTP 404s as `Unexpected` with a
        // "... does not exist" message rather than a *NotFound kind.
        let not_found = matches!(
            err.kind(),
            iceberg::ErrorKind::TableNotFound | iceberg::ErrorKind::NamespaceNotFound
        ) || err.message().contains("does not exist");
        let helper = if not_found { ErrorHelper::not_found() } else { ErrorHelper::io() };
        helper.message(err.to_string())
    }
}

fn iceberg_err(err: iceberg::Error) -> Error {
    ErrorHelper::from_iceberg(err).to_adbc()
}

fn invalid_arg(message: impl Into<String>) -> Error {
    Error::with_message_and_status(message.into(), Status::InvalidArguments)
}

// ---------------------------------------------------------------------------
// Tokio runtime
// ---------------------------------------------------------------------------

pub struct Runtime(tokio::runtime::Runtime);

impl Runtime {
    fn new() -> std::result::Result<Self, std::io::Error> {
        Ok(Self(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?,
        ))
    }

    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.0.block_on(f)
    }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct IcebergDriver {}

impl Driver for IcebergDriver {
    type DatabaseType = IcebergDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(IcebergDatabase {
            opts: HashMap::new(),
        })
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut db = self.new_database()?;
        for (key, value) in opts {
            db.set_option(key, value)?;
        }
        Ok(db)
    }
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

pub struct IcebergDatabase {
    opts: HashMap<String, String>,
}

impl IcebergDatabase {
    fn opt(&self, key: &str) -> Option<&str> {
        self.opts.get(key).map(|s| s.as_str())
    }

    fn require_opt(&self, key: &str) -> Result<&str> {
        self.opt(key).ok_or_else(|| invalid_arg(format!("required option '{key}' not set")))
    }

    fn build_catalog_props(&self) -> Result<HashMap<String, String>> {
        let mut props = HashMap::new();
        props.insert("uri".to_string(), self.require_opt("uri")?.to_string());

        let mappings = [
            ("adbc.iceberg.warehouse", "warehouse"),
            ("adbc.iceberg.auth.token", "token"),
            ("adbc.iceberg.auth.credential", "credential"),
            ("adbc.iceberg.auth.scope", "scope"),
            ("adbc.iceberg.auth.uri", "oauth2-server-uri"),
            ("adbc.iceberg.s3.endpoint", "s3.endpoint"),
            ("adbc.iceberg.s3.region", "s3.region"),
            ("adbc.iceberg.s3.access_key", "s3.access-key-id"),
            ("adbc.iceberg.s3.secret_key", "s3.secret-access-key"),
        ];
        for (adbc_key, iceberg_key) in mappings {
            if let Some(v) = self.opt(adbc_key) {
                props.insert(iceberg_key.to_string(), v.to_string());
            }
        }

        Ok(props)
    }
}

impl Optionable for IcebergDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match value {
            OptionValue::String(v) => {
                self.opts.insert(key.as_ref().to_string(), v);
                Ok(())
            }
            _ => Err(invalid_arg(format!("option '{key:?}' must be a string"))),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        self.opts.get(key.as_ref()).cloned().ok_or_else(|| {
            Error::with_message_and_status(format!("option '{key:?}' not set"), Status::NotFound)
        })
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status("not supported", Status::NotFound))
    }
    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status("not supported", Status::NotFound))
    }
    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status("not supported", Status::NotFound))
    }
}

impl Database for IcebergDatabase {
    type ConnectionType = IcebergConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let props = self.build_catalog_props()?;
        let name = self.opt("adbc.iceberg.catalog.name").unwrap_or("rest").to_string();

        let runtime = Arc::new(Runtime::new().map_err(|e| {
            Error::with_message_and_status(format!("failed to create runtime: {e}"), Status::Internal)
        })?);

        let storage_factory: Arc<dyn StorageFactory> = Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        });

        let catalog = runtime
            .block_on(
                RestCatalogBuilder::default()
                    .with_storage_factory(storage_factory)
                    .load(name.clone(), props),
            )
            .map_err(iceberg_err)?;

        Ok(IcebergConnection {
            runtime,
            catalog: Arc::new(catalog),
            catalog_name: name,
            current_db_schema: Arc::new(RwLock::new(None)),
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut conn = self.new_connection()?;
        for (key, value) in opts {
            conn.set_option(key, value)?;
        }
        Ok(conn)
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

pub struct IcebergConnection {
    runtime: Arc<Runtime>,
    catalog: Arc<dyn Catalog>,
    catalog_name: String,
    /// Namespace for unqualified table names. Shared with statements so that
    /// changing it on the connection affects statements created earlier.
    current_db_schema: Arc<RwLock<Option<String>>>,
}

impl IcebergConnection {
    fn resolve_namespace(&self, db_schema: Option<&str>) -> String {
        resolve_namespace(db_schema, &self.current_db_schema)
    }

    fn check_catalog(&self, catalog: Option<&str>) -> Result<()> {
        match catalog {
            Some(c) if c != self.catalog_name => Err(Error::with_message_and_status(
                format!("catalog '{c}' not found (configured catalog is '{}')", self.catalog_name),
                Status::NotFound,
            )),
            _ => Ok(()),
        }
    }

    fn objects(&self) -> IcebergObjects {
        IcebergObjects {
            runtime: self.runtime.clone(),
            catalog: self.catalog.clone(),
            catalog_name: self.catalog_name.clone(),
        }
    }
}

impl Optionable for IcebergConnection {
    type Option = OptionConnection;
    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key.as_ref() {
            ADBC_CONNECTION_OPTION_AUTOCOMMIT => Ok(()),
            ADBC_CONNECTION_OPTION_CURRENT_CATALOG => {
                let OptionValue::String(name) = value else {
                    return Err(invalid_arg("current catalog must be a string"));
                };
                if name != self.catalog_name {
                    return Err(invalid_arg(format!(
                        "catalog '{name}' does not match configured catalog '{}'",
                        self.catalog_name
                    )));
                }
                Ok(())
            }
            ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => {
                let OptionValue::String(name) = value else {
                    return Err(invalid_arg("current db_schema must be a string"));
                };
                *self.current_db_schema.write().unwrap() = Some(name).filter(|s| !s.is_empty());
                Ok(())
            }
            _ => Err(Error::with_message_and_status(format!("unsupported: {key:?}"), Status::NotFound)),
        }
    }
    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key.as_ref() {
            ADBC_CONNECTION_OPTION_CURRENT_CATALOG => Ok(self.catalog_name.clone()),
            ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => Ok(self.resolve_namespace(None)),
            _ => Err(Error::with_message_and_status(format!("unsupported: {key:?}"), Status::NotFound)),
        }
    }
    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status("not supported", Status::NotFound))
    }
    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status("not supported", Status::NotFound))
    }
    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status("not supported", Status::NotFound))
    }
}

impl Connection for IcebergConnection {
    type StatementType = IcebergStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(IcebergStatement {
            runtime: self.runtime.clone(),
            catalog: self.catalog.clone(),
            current_db_schema: self.current_db_schema.clone(),
            sql_query: None,
            scan_opts: ScanOptions::default(),
        })
    }

    fn cancel(&mut self) -> Result<()> { Ok(()) }
    fn get_info(&self, codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader + Send> {
        let mut registry = InfoRegistry::new();
        registry.add_string(InfoCode::VendorName, "Apache Iceberg");
        registry.add_string(InfoCode::DriverName, "ADBC Iceberg Driver (Rust)");
        registry.add_string(InfoCode::DriverVersion, env!("CARGO_PKG_VERSION"));
        Ok(registry.get_info(codes).build())
    }
    fn get_objects(&self, depth: ObjectDepth, catalog: Option<&str>, db_schema: Option<&str>, table_name: Option<&str>, table_type: Option<Vec<&str>>, column_name: Option<&str>) -> Result<impl RecordBatchReader + Send> {
        Ok(driverbase::get_objects::get_objects(
            self.objects(), depth, catalog, db_schema, table_name, table_type, column_name,
        ))
    }
    fn get_table_schema(&self, catalog: Option<&str>, db_schema: Option<&str>, table_name: &str) -> Result<Schema> {
        self.check_catalog(catalog)?;
        let namespace = self.resolve_namespace(db_schema);
        let table = load_table(&self.runtime, self.catalog.as_ref(), &namespace, table_name)?;
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).map_err(iceberg_err)
    }
    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        let types: ArrayRef = Arc::new(StringArray::from(vec![TABLE_TYPE]));
        let batch = RecordBatch::try_new(GET_TABLE_TYPES_SCHEMA.clone(), vec![types])?;
        Ok(RecordBatchIterator::new(vec![Ok(batch)], GET_TABLE_TYPES_SCHEMA.clone()))
    }
    fn read_partition(&self, partition: impl AsRef<[u8]>) -> Result<impl RecordBatchReader + Send> {
        let descriptor: PartitionDescriptor = serde_json::from_slice(partition.as_ref())
            .map_err(|e| invalid_arg(format!("invalid partition descriptor: {e}")))?;

        let parsed = parse_query(&descriptor.query)?;

        let table = load_table(
            &self.runtime, self.catalog.as_ref(),
            &descriptor.namespace, &descriptor.table_name,
        )?;
        let schema = output_schema(&table, descriptor.snapshot_id, &parsed)?;

        let stream = self.runtime.block_on(async {
            let scan = build_scan(&table, &parsed, descriptor.snapshot_id, descriptor.batch_size)?;

            // Plan all files, then filter to just the one matching our descriptor.
            use futures::TryStreamExt;
            let tasks: Vec<iceberg::scan::FileScanTask> = scan
                .plan_files()
                .await
                .map_err(iceberg_err)?
                .try_collect()
                .await
                .map_err(iceberg_err)?;

            let matching: Vec<_> = tasks
                .into_iter()
                .filter(|t| t.data_file_path == descriptor.data_file_path)
                .collect();

            let file_io = table.file_io().clone();
            let mut reader_builder = iceberg::arrow::ArrowReaderBuilder::new(file_io);
            if let Some(batch_size) = descriptor.batch_size {
                reader_builder = reader_builder.with_batch_size(batch_size);
            }
            let task_stream = futures::stream::iter(matching.into_iter().map(Ok));
            reader_builder.build().read(Box::pin(task_stream)).map_err(iceberg_err)
        })?;

        Ok(StreamingReader::new(Arc::new(schema), stream, self.runtime.clone()))
    }
    fn get_statistic_names(&self) -> Result<impl RecordBatchReader + Send> {
        Err::<BatchReader, _>(ErrorHelper::not_implemented().message("get_statistic_names").to_adbc())
    }
    fn get_statistics(&self, _catalog: Option<&str>, _db_schema: Option<&str>, _table_name: Option<&str>, _approximate: bool) -> Result<impl RecordBatchReader + Send> {
        Err::<BatchReader, _>(ErrorHelper::not_implemented().message("get_statistics").to_adbc())
    }
    fn commit(&mut self) -> Result<()> { Err(ErrorHelper::not_implemented().message("commit").to_adbc()) }
    fn rollback(&mut self) -> Result<()> { Err(ErrorHelper::not_implemented().message("rollback").to_adbc()) }
}

// ---------------------------------------------------------------------------
// Catalog enumeration (GetObjects)
// ---------------------------------------------------------------------------

/// Backs driverbase's GetObjects builder. Iceberg namespaces map to ADBC
/// db_schemas; the configured catalog is the only ADBC catalog.
struct IcebergObjects {
    runtime: Arc<Runtime>,
    catalog: Arc<dyn Catalog>,
    catalog_name: String,
}

type ObjectsResult<T> = std::result::Result<T, driverbase::error::Error<ErrorHelper>>;

impl IcebergObjects {
    fn list_tables(&self, db_schema: &str, table_filter: Option<&str>, table_type_filter: Option<&[String]>) -> ObjectsResult<Vec<TableIdent>> {
        if let Some(types) = table_type_filter
            && !types.iter().any(|t| t.eq_ignore_ascii_case(TABLE_TYPE))
        {
            return Ok(vec![]);
        }
        let ns = NamespaceIdent::from_strs(db_schema.split('.')).map_err(ErrorHelper::from_iceberg)?;
        let tables = self.runtime
            .block_on(self.catalog.list_tables(&ns))
            .map_err(ErrorHelper::from_iceberg)?;
        Ok(tables.into_iter().filter(|t| matches_filter(table_filter, t.name())).collect())
    }
}

impl GetObjectsImpl<ErrorHelper> for IcebergObjects {
    fn get_catalogs(&self, filter: Option<&str>) -> ObjectsResult<Vec<String>> {
        Ok(if matches_filter(filter, &self.catalog_name) {
            vec![self.catalog_name.clone()]
        } else {
            vec![]
        })
    }

    fn get_db_schemas(&self, _catalog: &str, filter: Option<&str>) -> ObjectsResult<Vec<String>> {
        let namespaces = self.runtime
            .block_on(self.catalog.list_namespaces(None))
            .map_err(ErrorHelper::from_iceberg)?;
        Ok(namespaces
            .into_iter()
            .map(|ns| ns.join("."))
            .filter(|name| matches_filter(filter, name))
            .collect())
    }

    fn get_tables(&self, _catalog: &str, db_schema: &str, table_filter: Option<&str>, table_type_filter: Option<&[String]>) -> ObjectsResult<Vec<TableInfo>> {
        Ok(self
            .list_tables(db_schema, table_filter, table_type_filter)?
            .into_iter()
            .map(|t| TableInfo { table_name: t.name().to_string(), table_type: TABLE_TYPE.to_string() })
            .collect())
    }

    fn get_columns(&self, _catalog: &str, db_schema: &str, table_filter: Option<&str>, table_type_filter: Option<&[String]>, column_filter: Option<&str>) -> ObjectsResult<Vec<TableAndColumnInfo>> {
        let mut result = Vec::new();
        for ident in self.list_tables(db_schema, table_filter, table_type_filter)? {
            let table = self.runtime
                .block_on(self.catalog.load_table(&ident))
                .map_err(ErrorHelper::from_iceberg)?;
            let columns = table
                .metadata()
                .current_schema()
                .as_struct()
                .fields()
                .iter()
                .filter(|f| matches_filter(column_filter, &f.name))
                .map(|f| ColumnInfo { column_name: f.name.clone() })
                .collect();
            result.push(TableAndColumnInfo {
                table: TableInfo { table_name: ident.name().to_string(), table_type: TABLE_TYPE.to_string() },
                columns,
            });
        }
        Ok(result)
    }
}

fn matches_filter(pattern: Option<&str>, value: &str) -> bool {
    pattern.is_none_or(|p| like_match(p.as_bytes(), value.as_bytes()))
}

/// SQL LIKE matching: `%` matches any sequence, `_` matches one character.
fn like_match(pattern: &[u8], value: &[u8]) -> bool {
    let (mut p, mut v) = (0, 0);
    // Position of the last `%` seen and the value index it is currently matched up to.
    let mut backtrack: Option<(usize, usize)> = None;
    while v < value.len() {
        if p < pattern.len() && (pattern[p] == b'_' || pattern[p] == value[v]) {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == b'%' {
            backtrack = Some((p, v));
            p += 1;
        } else if let Some((bp, bv)) = backtrack {
            p = bp + 1;
            v = bv + 1;
            backtrack = Some((bp, bv + 1));
        } else {
            return false;
        }
    }
    pattern[p..].iter().all(|&c| c == b'%')
}

// ---------------------------------------------------------------------------
// Statement
// ---------------------------------------------------------------------------

/// Statement options that control which table state is scanned and how.
#[derive(Clone, Default)]
struct ScanOptions {
    snapshot_id: Option<i64>,
    as_of_timestamp: Option<i64>,
    branch: Option<String>,
    batch_size: Option<usize>,
}

impl ScanOptions {
    /// Resolve the time-travel options against the table to a concrete
    /// snapshot. `None` means the current snapshot.
    fn resolve_snapshot(&self, table: &Table) -> Result<Option<i64>> {
        let set = [self.snapshot_id.is_some(), self.as_of_timestamp.is_some(), self.branch.is_some()];
        if set.iter().filter(|&&b| b).count() > 1 {
            return Err(invalid_arg(format!(
                "only one of '{OPTION_SNAPSHOT_ID}', '{OPTION_AS_OF_TIMESTAMP}', '{OPTION_BRANCH}' may be set"
            )));
        }
        let metadata = table.metadata();
        if let Some(id) = self.snapshot_id {
            if metadata.snapshot_by_id(id).is_none() {
                return Err(Error::with_message_and_status(format!("snapshot {id} not found"), Status::NotFound));
            }
            return Ok(Some(id));
        }
        if let Some(ref branch) = self.branch {
            return metadata
                .snapshot_for_ref(branch)
                .map(|s| Some(s.snapshot_id()))
                .ok_or_else(|| Error::with_message_and_status(format!("branch or tag '{branch}' not found"), Status::NotFound));
        }
        if let Some(ts) = self.as_of_timestamp {
            return metadata
                .history()
                .iter()
                .filter(|entry| entry.timestamp_ms() <= ts)
                .max_by_key(|entry| entry.timestamp_ms())
                .map(|entry| Some(entry.snapshot_id))
                .ok_or_else(|| Error::with_message_and_status(format!("no snapshot exists as of timestamp {ts}"), Status::NotFound));
        }
        Ok(None)
    }
}

pub struct IcebergStatement {
    runtime: Arc<Runtime>,
    catalog: Arc<dyn Catalog>,
    current_db_schema: Arc<RwLock<Option<String>>>,
    sql_query: Option<String>,
    scan_opts: ScanOptions,
}

/// A query resolved to its table and the snapshot it will read.
struct ResolvedQuery {
    parsed: sqlparser::SelectStatement,
    namespace: String,
    table: Table,
    snapshot_id: Option<i64>,
}

impl IcebergStatement {
    fn query(&self) -> Result<&str> {
        self.sql_query
            .as_deref()
            .ok_or_else(|| Error::with_message_and_status("no query set", Status::InvalidState))
    }

    fn resolve(&self) -> Result<ResolvedQuery> {
        let parsed = parse_query(self.query()?)?;
        let namespace = resolve_namespace(parsed.schema.as_deref(), &self.current_db_schema);
        let table = load_table(&self.runtime, self.catalog.as_ref(), &namespace, &parsed.table)?;
        let snapshot_id = self.scan_opts.resolve_snapshot(&table)?;
        Ok(ResolvedQuery { parsed, namespace, table, snapshot_id })
    }
}

fn resolve_namespace(explicit: Option<&str>, current: &RwLock<Option<String>>) -> String {
    match explicit {
        Some(ns) => ns.to_string(),
        None => current.read().unwrap().clone().unwrap_or_else(|| DEFAULT_NAMESPACE.to_string()),
    }
}

fn parse_query(query: &str) -> Result<sqlparser::SelectStatement> {
    sqlparser::parse(query).map_err(|e| invalid_arg(format!("unsupported SQL: {e}")))
}

fn load_table(runtime: &Runtime, catalog: &dyn Catalog, namespace: &str, table_name: &str) -> Result<Table> {
    let ns = NamespaceIdent::from_strs(namespace.split('.')).map_err(iceberg_err)?;
    let ident = TableIdent::new(ns, table_name.to_string());
    runtime.block_on(catalog.load_table(&ident)).map_err(iceberg_err)
}

/// Build a scan for the parsed query at the given snapshot.
fn build_scan(table: &Table, parsed: &sqlparser::SelectStatement, snapshot_id: Option<i64>, batch_size: Option<usize>) -> Result<TableScan> {
    let mut builder = table.scan().with_batch_size(batch_size);
    if let Some(id) = snapshot_id {
        builder = builder.snapshot_id(id);
    }
    if !parsed.select_all {
        builder = builder.select(parsed.columns.iter().map(|s| s.as_str()));
    }
    if let Some(ref where_expr) = parsed.where_clause {
        if let Ok(pred) = convert_predicate(where_expr) {
            builder = builder.with_filter(pred);
        }
    }
    builder.build().map_err(iceberg_err)
}

/// The Arrow schema a query produces: the snapshot's table schema with the
/// query's projection applied.
fn output_schema(table: &Table, snapshot_id: Option<i64>, parsed: &sqlparser::SelectStatement) -> Result<Schema> {
    let metadata = table.metadata();
    let iceberg_schema = match snapshot_id.and_then(|id| metadata.snapshot_by_id(id)) {
        Some(snapshot) => snapshot.schema(metadata).map_err(iceberg_err)?,
        None => metadata.current_schema().clone(),
    };
    let schema = iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).map_err(iceberg_err)?;
    if parsed.select_all {
        return Ok(schema);
    }
    let indices = parsed
        .columns
        .iter()
        .map(|c| schema.index_of(c).map_err(|_| invalid_arg(format!("column '{c}' not found"))))
        .collect::<Result<Vec<_>>>()?;
    Ok(schema.project(&indices)?)
}

/// Convert parsed WHERE AST to iceberg Predicate.
fn convert_predicate(expr: &sqlparser::Expr) -> Result<Predicate> {
    match expr {
        sqlparser::Expr::Binary { column, op, value } => {
            let r = Reference::new(column);
            let d = convert_datum(value)?;
            Ok(match op {
                sqlparser::BinaryOp::Eq => r.equal_to(d),
                sqlparser::BinaryOp::Neq => r.not_equal_to(d),
                sqlparser::BinaryOp::Lt => r.less_than(d),
                sqlparser::BinaryOp::Lte => r.less_than_or_equal_to(d),
                sqlparser::BinaryOp::Gt => r.greater_than(d),
                sqlparser::BinaryOp::Gte => r.greater_than_or_equal_to(d),
            })
        }
        sqlparser::Expr::And(left, right) => {
            Ok(convert_predicate(left)?.and(convert_predicate(right)?))
        }
        sqlparser::Expr::Or(left, right) => {
            Ok(convert_predicate(left)?.or(convert_predicate(right)?))
        }
        sqlparser::Expr::Not(inner) => Ok(convert_predicate(inner)?.negate()),
        sqlparser::Expr::IsNull(col) => Ok(Reference::new(col).is_null()),
        sqlparser::Expr::IsNotNull(col) => Ok(Reference::new(col).is_not_null()),
    }
}

fn convert_datum(val: &sqlparser::LiteralValue) -> Result<Datum> {
    match val {
        sqlparser::LiteralValue::Int(v) => Ok(Datum::long(*v)),
        sqlparser::LiteralValue::Float(v) => Ok(Datum::double(*v)),
        sqlparser::LiteralValue::String(s) => Ok(Datum::string(s)),
        sqlparser::LiteralValue::Bool(b) => Ok(Datum::bool(*b)),
    }
}

/// Parse an integer option given either as an int or a string. An empty
/// string clears the option.
fn parse_int_option(key: &str, value: OptionValue) -> Result<Option<i64>> {
    match value {
        OptionValue::Int(v) => Ok(Some(v)),
        OptionValue::String(s) if s.is_empty() => Ok(None),
        OptionValue::String(s) => s
            .trim()
            .parse()
            .map(Some)
            .map_err(|_| invalid_arg(format!("option '{key}' must be an integer, got '{s}'"))),
        _ => Err(invalid_arg(format!("option '{key}' must be an integer"))),
    }
}

fn option_not_set(key: &str) -> Error {
    Error::with_message_and_status(format!("option '{key}' not set"), Status::NotFound)
}

impl Optionable for IcebergStatement {
    type Option = OptionStatement;
    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key.as_ref() {
            OPTION_SNAPSHOT_ID => self.scan_opts.snapshot_id = parse_int_option(OPTION_SNAPSHOT_ID, value)?,
            OPTION_AS_OF_TIMESTAMP => self.scan_opts.as_of_timestamp = parse_int_option(OPTION_AS_OF_TIMESTAMP, value)?,
            OPTION_BRANCH => match value {
                OptionValue::String(s) => self.scan_opts.branch = Some(s).filter(|s| !s.is_empty()),
                _ => return Err(invalid_arg(format!("option '{OPTION_BRANCH}' must be a string"))),
            },
            OPTION_BATCH_SIZE => {
                self.scan_opts.batch_size = match parse_int_option(OPTION_BATCH_SIZE, value)? {
                    Some(v) if v > 0 => Some(v as usize),
                    Some(v) => return Err(invalid_arg(format!("option '{OPTION_BATCH_SIZE}' must be positive, got {v}"))),
                    None => None,
                }
            }
            _ => return Err(Error::with_message_and_status(format!("unsupported: {key:?}"), Status::NotFound)),
        }
        Ok(())
    }
    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let key = key.as_ref();
        match key {
            OPTION_BRANCH => self.scan_opts.branch.clone().ok_or_else(|| option_not_set(key)),
            OPTION_SNAPSHOT_ID | OPTION_AS_OF_TIMESTAMP | OPTION_BATCH_SIZE => {
                self.get_option_int(key.into()).map(|v| v.to_string())
            }
            _ => Err(Error::with_message_and_status(format!("unsupported: {key:?}"), Status::NotFound)),
        }
    }
    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status("not supported", Status::NotFound))
    }
    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        let key = key.as_ref();
        let value = match key {
            OPTION_SNAPSHOT_ID => self.scan_opts.snapshot_id,
            OPTION_AS_OF_TIMESTAMP => self.scan_opts.as_of_timestamp,
            OPTION_BATCH_SIZE => self.scan_opts.batch_size.map(|v| v as i64),
            _ => return Err(Error::with_message_and_status(format!("unsupported: {key:?}"), Status::NotFound)),
        };
        value.ok_or_else(|| option_not_set(key))
    }
    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status("not supported", Status::NotFound))
    }
}

impl Statement for IcebergStatement {
    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        let q = self.resolve()?;
        let schema = output_schema(&q.table, q.snapshot_id, &q.parsed)?;
        let scan = build_scan(&q.table, &q.parsed, q.snapshot_id, self.scan_opts.batch_size)?;

        // Only plan here; batches are read lazily by the reader.
        let stream = self.runtime.block_on(scan.to_arrow()).map_err(iceberg_err)?;
        Ok(StreamingReader::new(Arc::new(schema), stream, self.runtime.clone()))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Err(ErrorHelper::not_implemented().message("execute_update").to_adbc())
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let q = self.resolve()?;
        output_schema(&q.table, q.snapshot_id, &q.parsed)
    }

    fn execute_partitions(&mut self) -> Result<adbc_core::PartitionedResult> {
        let query = self.query()?.to_string();
        let q = self.resolve()?;
        let schema = output_schema(&q.table, q.snapshot_id, &q.parsed)?;
        let scan = build_scan(&q.table, &q.parsed, q.snapshot_id, self.scan_opts.batch_size)?;
        // Pin partitions to the snapshot actually planned, so readers see the
        // same table state even if the table is updated before they run.
        let snapshot_id = scan.snapshot().map(|s| s.snapshot_id());

        let tasks: Vec<iceberg::scan::FileScanTask> = self.runtime.block_on(async {
            use futures::TryStreamExt;
            scan.plan_files()
                .await
                .map_err(iceberg_err)?
                .try_collect()
                .await
                .map_err(iceberg_err)
        })?;

        let partitions = tasks
            .into_iter()
            .map(|task| {
                let descriptor = PartitionDescriptor {
                    namespace: q.namespace.clone(),
                    table_name: q.parsed.table.clone(),
                    query: query.clone(),
                    data_file_path: task.data_file_path,
                    snapshot_id,
                    batch_size: self.scan_opts.batch_size,
                };
                serde_json::to_vec(&descriptor).map_err(|e| {
                    Error::with_message_and_status(format!("failed to serialize descriptor: {e}"), Status::Internal)
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(adbc_core::PartitionedResult {
            schema,
            partitions,
            rows_affected: -1,
        })
    }
    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(ErrorHelper::not_implemented().message("get_parameter_schema").to_adbc())
    }
    fn prepare(&mut self) -> Result<()> { Ok(()) }
    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(ErrorHelper::not_implemented().message("set_substrait_plan").to_adbc())
    }
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Err(ErrorHelper::not_implemented().message("bind").to_adbc())
    }
    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Err(ErrorHelper::not_implemented().message("bind_stream").to_adbc())
    }
    fn cancel(&mut self) -> Result<()> { Ok(()) }
}

// ---------------------------------------------------------------------------
// RecordBatch readers
// ---------------------------------------------------------------------------

use std::pin::Pin;
use futures::stream::BoxStream;
use futures::StreamExt;

/// Streaming reader that pulls batches from an async iceberg stream on each next() call.
pub struct StreamingReader {
    schema: Arc<Schema>,
    stream: Pin<Box<BoxStream<'static, std::result::Result<RecordBatch, arrow_schema::ArrowError>>>>,
    runtime: Arc<Runtime>,
    first: Option<RecordBatch>,
    done: bool,
}

// Safety: the stream is Send (iceberg guarantees this) and we only access it
// through block_on which is synchronized.
unsafe impl Send for StreamingReader {}

impl StreamingReader {
    fn new(
        fallback_schema: Arc<Schema>,
        stream: iceberg::scan::ArrowRecordBatchStream,
        runtime: Arc<Runtime>,
    ) -> Self {
        let mut mapped: BoxStream<'static, std::result::Result<RecordBatch, arrow_schema::ArrowError>> =
            stream
                .map(|r| r.map_err(|e| arrow_schema::ArrowError::ExternalError(Box::new(e))))
                .boxed();

        // Read the first batch to get the actual (possibly projected) schema.
        let first = runtime.block_on(mapped.next());
        let (schema, first_batch) = match first {
            Some(Ok(batch)) => {
                let s = batch.schema();
                (s, Some(batch))
            }
            _ => (fallback_schema, None),
        };

        Self {
            schema,
            stream: Box::pin(mapped),
            runtime,
            first: first_batch,
            done: false,
        }
    }
}

impl Iterator for StreamingReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Return buffered first batch before streaming.
        if let Some(batch) = self.first.take() {
            return Some(Ok(batch));
        }
        if self.done {
            return None;
        }
        match self.runtime.block_on(self.stream.next()) {
            Some(Ok(batch)) => Some(Ok(batch)),
            Some(Err(e)) => {
                self.done = true;
                Some(Err(e))
            }
            None => {
                self.done = true;
                None
            }
        }
    }
}

impl RecordBatchReader for StreamingReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

/// Simple reader for empty results.
pub struct BatchReader {
    schema: Arc<Schema>,
    done: bool,
}

impl BatchReader {
    fn new_empty(schema: Arc<Schema>) -> Self {
        Self { schema, done: true }
    }
}

impl Iterator for BatchReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> { None }
}

impl RecordBatchReader for BatchReader {
    fn schema(&self) -> Arc<Schema> { self.schema.clone() }
}

// ---------------------------------------------------------------------------
// C ABI export
// ---------------------------------------------------------------------------

adbc_ffi::export_driver!(AdbcDriverIcebergInit, IcebergDriver);

#[cfg(test)]
mod tests {
    use super::*;

    fn like(p: &str, v: &str) -> bool {
        like_match(p.as_bytes(), v.as_bytes())
    }

    #[test]
    fn like_matching() {
        assert!(like("%", ""));
        assert!(like("%", "anything"));
        assert!(like("abc", "abc"));
        assert!(!like("abc", "abcd"));
        assert!(like("ab%", "abcd"));
        assert!(like("%cd", "abcd"));
        assert!(like("%bc%", "abcd"));
        assert!(like("a_c", "abc"));
        assert!(!like("a_c", "ac"));
        assert!(like("a%c%e", "abcde"));
        assert!(like("a%%e", "ae"));
        assert!(!like("a%c%e", "abcdf"));
        assert!(like("%a%a", "aXaXa"));
    }

    #[test]
    fn filter_none_matches_everything() {
        assert!(matches_filter(None, "x"));
        assert!(!matches_filter(Some("y"), "x"));
    }
}
