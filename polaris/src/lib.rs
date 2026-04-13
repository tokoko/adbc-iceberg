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

mod auth;
mod delta_reader;
mod polaris_api;
mod s3_config;
mod sqlparser;

use s3_config::S3Config;

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct ErrorHelper {}

impl driverbase::error::ErrorHelper for ErrorHelper {
    const NAME: &'static str = "polaris";
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
pub struct PolarisDriver {}

impl Driver for PolarisDriver {
    type DatabaseType = PolarisDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(PolarisDatabase {
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

pub struct PolarisDatabase {
    opts: HashMap<String, String>,
}

impl PolarisDatabase {
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

    fn s3_config(&self) -> S3Config {
        S3Config {
            endpoint: self.opt("adbc.polaris.s3.endpoint").map(|s| s.to_string()),
            region: self.opt("adbc.polaris.s3.region").map(|s| s.to_string()),
            access_key: self.opt("adbc.polaris.s3.access_key").map(|s| s.to_string()),
            secret_key: self.opt("adbc.polaris.s3.secret_key").map(|s| s.to_string()),
        }
    }
}

impl Optionable for PolarisDatabase {
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

impl Database for PolarisDatabase {
    type ConnectionType = PolarisConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let base_url = self.require_opt("uri")?.to_string();
        let warehouse = self.opt("adbc.polaris.warehouse").unwrap_or("rest").to_string();
        let scope = self.opt("adbc.polaris.scope").unwrap_or("PRINCIPAL_ROLE:ALL").to_string();
        let s3_config = self.s3_config();

        let runtime = Arc::new(Runtime::new().map_err(|e| {
            Error::with_message_and_status(format!("failed to create runtime: {e}"), Status::Internal)
        })?);

        // Acquire OAuth2 token if credentials are provided
        let credential = self.opt("adbc.polaris.credential");
        let token = if let Some(cred) = credential {
            let parts: Vec<&str> = cred.splitn(2, ':').collect();
            if parts.len() != 2 {
                return Err(Error::with_message_and_status(
                    "credential must be in 'client_id:client_secret' format",
                    Status::InvalidArguments,
                ));
            }
            let token = runtime
                .block_on(auth::acquire_token(&base_url, parts[0], parts[1], &scope))
                .map_err(|e| Error::with_message_and_status(
                    format!("OAuth2 token acquisition failed: {e}"),
                    Status::IO,
                ))?;
            Some(token)
        } else {
            None
        };

        // Build Iceberg REST catalog
        let catalog_uri = format!("{base_url}/api/catalog");
        let mut props = HashMap::new();
        props.insert("uri".to_string(), catalog_uri);
        props.insert("warehouse".to_string(), warehouse.clone());
        if let Some(ref cred) = credential {
            props.insert("credential".to_string(), cred.to_string());
        }
        props.insert("scope".to_string(), scope);
        for (k, v) in s3_config.to_iceberg_props() {
            props.insert(k, v);
        }

        let storage_factory: Arc<dyn StorageFactory> = Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        });

        let catalog = runtime
            .block_on(
                RestCatalogBuilder::default()
                    .with_storage_factory(storage_factory)
                    .load(warehouse.clone(), props),
            )
            .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;

        // Build HTTP client for Generic Tables API
        let mut http_builder = reqwest::Client::builder();
        if let Some(ref token) = token {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|e| Error::with_message_and_status(
                        format!("invalid token: {e}"),
                        Status::Internal,
                    ))?,
            );
            http_builder = http_builder.default_headers(headers);
        }
        let http_client = http_builder.build().map_err(|e| {
            Error::with_message_and_status(format!("failed to create HTTP client: {e}"), Status::Internal)
        })?;

        Ok(PolarisConnection {
            runtime,
            catalog: Arc::new(catalog),
            http_client,
            base_url,
            warehouse,
            s3_config,
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

pub struct PolarisConnection {
    runtime: Arc<Runtime>,
    catalog: Arc<dyn Catalog>,
    http_client: reqwest::Client,
    base_url: String,
    warehouse: String,
    s3_config: S3Config,
}

impl Optionable for PolarisConnection {
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

impl Connection for PolarisConnection {
    type StatementType = PolarisStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(PolarisStatement {
            runtime: self.runtime.clone(),
            catalog: self.catalog.clone(),
            http_client: self.http_client.clone(),
            base_url: self.base_url.clone(),
            warehouse: self.warehouse.clone(),
            s3_config: self.s3_config.clone(),
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
    fn read_partition(&self, _partition: impl AsRef<[u8]>) -> Result<impl RecordBatchReader + Send> {
        Err::<BatchReader, _>(ErrorHelper::not_implemented().message("read_partition").to_adbc())
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

pub struct PolarisStatement {
    runtime: Arc<Runtime>,
    catalog: Arc<dyn Catalog>,
    http_client: reqwest::Client,
    base_url: String,
    warehouse: String,
    s3_config: S3Config,
    sql_query: Option<String>,
}

fn load_iceberg_table(runtime: &Runtime, catalog: &dyn Catalog, namespace: &str, table_name: &str) -> Result<Table> {
    runtime.block_on(async {
        let ident = iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new(namespace.to_string()),
            table_name.to_string(),
        );
        catalog.load_table(&ident).await.map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())
    })
}

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
        sqlparser::Expr::And(left, right) => Ok(convert_predicate(left)?.and(convert_predicate(right)?)),
        sqlparser::Expr::Or(left, right) => Ok(convert_predicate(left)?.or(convert_predicate(right)?)),
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

fn execute_iceberg(
    runtime: &Arc<Runtime>,
    catalog: &Arc<dyn Catalog>,
    parsed: &sqlparser::SelectStatement,
) -> Result<StreamingReader> {
    let namespace = parsed.schema.as_deref().unwrap_or("default");
    let table = load_iceberg_table(runtime, catalog.as_ref(), namespace, &parsed.table)?;

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
        let schema = iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;
        let stream = scan.to_arrow().await.map_err(|e| ErrorHelper::from_iceberg(e).to_adbc())?;
        Ok::<_, Error>((schema, stream))
    })?;

    Ok(StreamingReader::new(Arc::new(schema), stream, runtime.clone()))
}

fn execute_delta(
    runtime: &Arc<Runtime>,
    http_client: &reqwest::Client,
    base_url: &str,
    warehouse: &str,
    s3_config: &S3Config,
    parsed: &sqlparser::SelectStatement,
) -> Result<VecReader> {
    let namespace = parsed.schema.as_deref().unwrap_or("default");

    // Fetch table metadata from Polaris Generic Tables API
    let generic_table = runtime
        .block_on(polaris_api::load_generic_table(
            http_client, base_url, warehouse, namespace, &parsed.table,
        ))
        .map_err(|e| Error::with_message_and_status(
            format!("failed to load generic table: {e}"),
            Status::IO,
        ))?;

    let base_location = generic_table.base_location.ok_or_else(|| {
        Error::with_message_and_status("generic table has no base location", Status::IO)
    })?;

    let (schema, batches) = delta_reader::execute(runtime, &base_location, s3_config, parsed)?;
    Ok(VecReader::new(schema, batches))
}

impl Optionable for PolarisStatement {
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

impl Statement for PolarisStatement {
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

        // Auto-detect table format via Polaris Generic Tables API
        let format = self.runtime.block_on(polaris_api::detect_format(
            &self.http_client,
            &self.base_url,
            &self.warehouse,
            namespace,
            &parsed.table,
        ));

        match format {
            polaris_api::TableFormat::Iceberg => {
                let reader = execute_iceberg(&self.runtime, &self.catalog, &parsed)?;
                Ok(EitherReader::Streaming(reader))
            }
            polaris_api::TableFormat::Delta => {
                let reader = execute_delta(
                    &self.runtime,
                    &self.http_client,
                    &self.base_url,
                    &self.warehouse,
                    &self.s3_config,
                    &parsed,
                )?;
                Ok(EitherReader::Vec(reader))
            }
        }
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Err(ErrorHelper::not_implemented().message("execute_update").to_adbc())
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Err(ErrorHelper::not_implemented().message("execute_schema").to_adbc())
    }

    fn execute_partitions(&mut self) -> Result<adbc_core::PartitionedResult> {
        Err(ErrorHelper::not_implemented().message("execute_partitions").to_adbc())
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

/// Either a streaming (Iceberg) or vec-based (Delta) reader.
pub enum EitherReader {
    Streaming(StreamingReader),
    Vec(VecReader),
}

impl Iterator for EitherReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EitherReader::Streaming(r) => r.next(),
            EitherReader::Vec(r) => r.next(),
        }
    }
}

impl RecordBatchReader for EitherReader {
    fn schema(&self) -> Arc<Schema> {
        match self {
            EitherReader::Streaming(r) => r.schema(),
            EitherReader::Vec(r) => r.schema(),
        }
    }
}

unsafe impl Send for EitherReader {}

/// Streaming reader for Iceberg async streams.
pub struct StreamingReader {
    schema: Arc<Schema>,
    stream: Pin<Box<BoxStream<'static, std::result::Result<RecordBatch, arrow_schema::ArrowError>>>>,
    runtime: Arc<Runtime>,
    first: Option<RecordBatch>,
    done: bool,
}

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
    fn schema(&self) -> Arc<Schema> { self.schema.clone() }
}

/// Vec-based reader for Delta batches (already materialized).
pub struct VecReader {
    schema: Arc<Schema>,
    batches: std::vec::IntoIter<RecordBatch>,
}

impl VecReader {
    fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into_iter(),
        }
    }
}

impl Iterator for VecReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}

impl RecordBatchReader for VecReader {
    fn schema(&self) -> Arc<Schema> { self.schema.clone() }
}

/// Empty reader stub.
pub struct BatchReader {
    schema: Arc<Schema>,
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

adbc_ffi::export_driver!(AdbcDriverPolarisInit, PolarisDriver);
