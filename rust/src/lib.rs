#![allow(refining_impl_trait)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_schema::Schema;

use adbc_core::{
    Connection, Database, Driver, Optionable, Statement,
    error::{Error, Result, Status},
    options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue},
};
use driverbase::error::ErrorHelper as _;

use iceberg::expr::{Predicate, Reference};
use iceberg::io::StorageFactory;
use iceberg::spec::Datum;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

mod sqlparser;

use serde::{Deserialize, Serialize};

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
        ErrorHelper::io().message(err.to_string())
    }
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
        self.opt(key).ok_or_else(|| {
            Error::with_message_and_status(
                format!("required option '{key}' not set"),
                Status::InvalidArguments,
            )
        })
    }

    fn build_catalog_props(&self) -> Result<HashMap<String, String>> {
        let mut props = HashMap::new();
        props.insert("uri".to_string(), self.require_opt("uri")?.to_string());

        let mappings = [
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
            _ => Err(Error::with_message_and_status(
                format!("option '{key:?}' must be a string"),
                Status::InvalidArguments,
            )),
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
                    .load(name, props),
            )
            .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;

        Ok(IcebergConnection {
            runtime,
            catalog: Arc::new(catalog),
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
}

impl Optionable for IcebergConnection {
    type Option = OptionConnection;
    fn set_option(&mut self, key: Self::Option, _value: OptionValue) -> Result<()> {
        match key.as_ref() {
            adbc_core::constants::ADBC_CONNECTION_OPTION_AUTOCOMMIT => Ok(()),
            _ => Err(Error::with_message_and_status(format!("unsupported: {key:?}"), Status::NotFound)),
        }
    }
    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        Err(Error::with_message_and_status(format!("unsupported: {key:?}"), Status::NotFound))
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
            sql_query: None,
        })
    }

    fn cancel(&mut self) -> Result<()> { Ok(()) }
    fn get_info(&self, _codes: Option<std::collections::HashSet<adbc_core::options::InfoCode>>) -> Result<impl RecordBatchReader + Send> {
        Err::<BatchReader, _>(ErrorHelper::not_implemented().message("get_info").to_adbc())
    }
    fn get_objects(&self, _depth: adbc_core::options::ObjectDepth, _catalog: Option<&str>, _db_schema: Option<&str>, _table_name: Option<&str>, _table_type: Option<Vec<&str>>, _column_name: Option<&str>) -> Result<impl RecordBatchReader + Send> {
        Err::<BatchReader, _>(ErrorHelper::not_implemented().message("get_objects").to_adbc())
    }
    fn get_table_schema(&self, _catalog: Option<&str>, _db_schema: Option<&str>, _table_name: &str) -> Result<Schema> {
        Err(ErrorHelper::not_implemented().message("get_table_schema").to_adbc())
    }
    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        Err::<BatchReader, _>(ErrorHelper::not_implemented().message("get_table_types").to_adbc())
    }
    fn read_partition(&self, partition: impl AsRef<[u8]>) -> Result<impl RecordBatchReader + Send> {
        let descriptor: PartitionDescriptor = serde_json::from_slice(partition.as_ref())
            .map_err(|e| Error::with_message_and_status(
                format!("invalid partition descriptor: {e}"), Status::InvalidArguments,
            ))?;

        let parsed = sqlparser::parse(&descriptor.query).map_err(|e| {
            Error::with_message_and_status(format!("unsupported SQL: {e}"), Status::InvalidArguments)
        })?;

        let table = load_table(
            &self.runtime, self.catalog.as_ref(),
            &descriptor.namespace, &descriptor.table_name,
        )?;

        let (schema, stream) = self.runtime.block_on(async {
            let mut builder = table.scan();
            if !parsed.select_all {
                builder = builder.select(parsed.columns.iter().map(|s| s.as_str()));
            }
            if let Some(ref where_expr) = parsed.where_clause {
                if let Ok(pred) = convert_predicate(where_expr) {
                    builder = builder.with_filter(pred);
                }
            }

            let scan = builder.build().map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;

            // Plan all files, then filter to just the one matching our descriptor.
            use futures::TryStreamExt;
            let tasks: Vec<iceberg::scan::FileScanTask> = scan
                .plan_files()
                .await
                .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?
                .try_collect()
                .await
                .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;

            let matching: Vec<_> = tasks
                .into_iter()
                .filter(|t| t.data_file_path == descriptor.data_file_path)
                .collect();

            let file_io = table.file_io().clone();
            let reader = iceberg::arrow::ArrowReaderBuilder::new(file_io).build();
            let task_stream = futures::stream::iter(matching.into_iter().map(Ok));
            let batch_stream = reader.read(Box::pin(task_stream))
                .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;

            let schema = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;

            Ok::<_, Error>((schema, batch_stream))
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
// Statement
// ---------------------------------------------------------------------------

pub struct IcebergStatement {
    runtime: Arc<Runtime>,
    catalog: Arc<dyn Catalog>,
    sql_query: Option<String>,
}

fn load_table(runtime: &Runtime, catalog: &dyn Catalog, namespace: &str, table_name: &str) -> Result<Table> {
    runtime.block_on(async {
        let ident = iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new(namespace.to_string()),
            table_name.to_string(),
        );
        catalog.load_table(&ident).await.map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())
    })
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

impl Optionable for IcebergStatement {
    type Option = OptionStatement;
    fn set_option(&mut self, key: Self::Option, _value: OptionValue) -> Result<()> {
        Err(Error::with_message_and_status(format!("unsupported: {key:?}"), Status::NotFound))
    }
    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        Err(Error::with_message_and_status(format!("unsupported: {key:?}"), Status::NotFound))
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

impl Statement for IcebergStatement {
    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        let query = self.sql_query.as_deref().ok_or_else(|| {
            Error::with_message_and_status("no query set", Status::InvalidState)
        })?;

        let parsed = sqlparser::parse(query).map_err(|e| {
            Error::with_message_and_status(format!("unsupported SQL: {e}"), Status::InvalidArguments)
        })?;

        let namespace = parsed.schema.as_deref().unwrap_or("default");
        let table = load_table(&self.runtime, self.catalog.as_ref(), namespace, &parsed.table)?;

        let runtime = self.runtime.clone();

        // Build scan and get the arrow stream — only plan, don't read yet.
        let (schema, stream) = runtime.block_on(async {
            let mut builder = table.scan();

            if !parsed.select_all {
                builder = builder.select(parsed.columns.iter().map(|s| s.as_str()));
            }

            if let Some(ref where_expr) = parsed.where_clause {
                if let Ok(pred) = convert_predicate(where_expr) {
                    builder = builder.with_filter(pred);
                }
            }

            let scan = builder.build().map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;
            // Get the full schema — the stream will produce projected batches.
            // We'll update the schema from the first batch if needed.
            let schema = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;
            let stream = scan.to_arrow().await.map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;
            Ok::<_, Error>((schema, stream))
        })?;

        Ok(StreamingReader::new(Arc::new(schema), stream, runtime))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Err(ErrorHelper::not_implemented().message("execute_update").to_adbc())
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let query = self.sql_query.as_deref().ok_or_else(|| {
            Error::with_message_and_status("no query set", Status::InvalidState)
        })?;
        let parsed = sqlparser::parse(query).map_err(|e| {
            Error::with_message_and_status(format!("unsupported SQL: {e}"), Status::InvalidArguments)
        })?;
        let namespace = parsed.schema.as_deref().unwrap_or("default");
        let table = load_table(&self.runtime, self.catalog.as_ref(), namespace, &parsed.table)?;
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())
    }

    fn execute_partitions(&mut self) -> Result<adbc_core::PartitionedResult> {
        let query = self.sql_query.as_deref().ok_or_else(|| {
            Error::with_message_and_status("no query set", Status::InvalidState)
        })?;

        let parsed = sqlparser::parse(query).map_err(|e| {
            Error::with_message_and_status(format!("unsupported SQL: {e}"), Status::InvalidArguments)
        })?;

        let namespace = parsed.schema.as_deref().unwrap_or("default");
        let table = load_table(&self.runtime, self.catalog.as_ref(), namespace, &parsed.table)?;

        let (schema, partitions) = self.runtime.block_on(async {
            let mut builder = table.scan();
            if !parsed.select_all {
                builder = builder.select(parsed.columns.iter().map(|s| s.as_str()));
            }
            if let Some(ref where_expr) = parsed.where_clause {
                if let Ok(pred) = convert_predicate(where_expr) {
                    builder = builder.with_filter(pred);
                }
            }

            let scan = builder.build().map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;

            let schema = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;

            use futures::TryStreamExt;
            let tasks: Vec<iceberg::scan::FileScanTask> = scan
                .plan_files()
                .await
                .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?
                .try_collect()
                .await
                .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;

            let mut partition_data = Vec::with_capacity(tasks.len());
            for task in tasks {
                let descriptor = PartitionDescriptor {
                    namespace: namespace.to_string(),
                    table_name: parsed.table.clone(),
                    query: query.to_string(),
                    data_file_path: task.data_file_path.clone(),
                };
                let bytes = serde_json::to_vec(&descriptor).map_err(|e| {
                    Error::with_message_and_status(format!("failed to serialize descriptor: {e}"), Status::Internal)
                })?;
                partition_data.push(bytes);
            }

            Ok::<_, Error>((schema, partition_data))
        })?;

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
