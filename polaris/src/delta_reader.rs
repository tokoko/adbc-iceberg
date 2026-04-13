use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use delta_kernel::engine::arrow_data::EngineDataArrowExt;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::snapshot::Snapshot;

use adbc_core::error::{Error, Status};

use crate::s3_config::S3Config;
use crate::sqlparser::SelectStatement;

/// Read a Delta table from the given base location using delta_kernel.
pub fn execute(
    runtime: &crate::Runtime,
    base_location: &str,
    s3_config: &S3Config,
    parsed: &SelectStatement,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), Error> {
    runtime.block_on(execute_async(base_location, s3_config, parsed))
}

async fn execute_async(
    base_location: &str,
    s3_config: &S3Config,
    parsed: &SelectStatement,
) -> Result<(Arc<Schema>, Vec<RecordBatch>), Error> {
    // Ensure trailing slash so Url::join appends rather than replacing last segment
    let loc = if base_location.ends_with('/') {
        base_location.to_string()
    } else {
        format!("{base_location}/")
    };
    let url = url::Url::parse(&loc)
        .map_err(|e| delta_err(format!("invalid location: {e}")))?;

    // Build object_store from S3 config
    let bucket = url.host_str().unwrap_or("");
    let mut builder = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false);

    if let Some(ref endpoint) = s3_config.endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(ref region) = s3_config.region {
        builder = builder.with_region(region);
    }
    if let Some(ref key) = s3_config.access_key {
        builder = builder.with_access_key_id(key);
    }
    if let Some(ref secret) = s3_config.secret_key {
        builder = builder.with_secret_access_key(secret);
    }

    let store: Arc<dyn object_store::ObjectStore> = Arc::new(
        builder.build().map_err(|e| delta_err(format!("build object store: {e}")))?
    );

    // Wrap with PrefixStore since delta_kernel passes full s3://bucket/path URLs
    // but object_store::AmazonS3 expects paths relative to bucket root
    let engine = Arc::new(DefaultEngineBuilder::new(store).build());

    let snapshot = Snapshot::builder_for(url)
        .build(engine.as_ref())
        .map_err(|e| delta_err(format!("build snapshot: {e}")))?;

    // Build scan with optional column projection
    let mut scan_builder = snapshot.clone().scan_builder();
    if !parsed.select_all && !parsed.columns.is_empty() {
        let schema_ref = snapshot.schema();
        let projected = select_columns(&schema_ref, &parsed.columns)?;
        scan_builder = scan_builder.with_schema(Arc::new(projected));
    }
    let scan = scan_builder.build()
        .map_err(|e| delta_err(format!("build scan: {e}")))?;

    // Execute scan — delta_kernel handles selection vectors internally
    let results = scan.execute(engine.clone())
        .map_err(|e| delta_err(format!("execute scan: {e}")))?;

    let mut batches = Vec::new();
    let mut schema: Option<Arc<Schema>> = None;

    for result in results {
        let batch: RecordBatch = result
            .map_err(|e| delta_err(format!("scan result: {e}")))?
            .try_into_record_batch()
            .map_err(|e| delta_err(format!("convert to RecordBatch: {e}")))?;

        if schema.is_none() {
            schema = Some(batch.schema());
        }
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    let schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));
    Ok((schema, batches))
}

fn delta_err(msg: String) -> Error {
    Error::with_message_and_status(msg, Status::IO)
}

/// Select a subset of columns from a delta_kernel schema.
fn select_columns(
    schema: &delta_kernel::schema::Schema,
    columns: &[String],
) -> Result<delta_kernel::schema::Schema, Error> {
    let fields: Vec<_> = columns
        .iter()
        .filter_map(|name| schema.fields().find(|f| f.name() == name).cloned())
        .collect();
    if fields.is_empty() {
        return Err(Error::with_message_and_status(
            "none of the requested columns found in table",
            Status::InvalidArguments,
        ));
    }
    delta_kernel::schema::Schema::try_new(fields)
        .map_err(|e| delta_err(format!("schema projection: {e}")))
}
