//! Raw S3 read benchmark: opendal vs object_store
//! Reads the same Parquet file both ways, no iceberg involved.
//!
//! Usage:
//!     cargo run --release --example raw_read

use std::collections::HashMap;
use std::time::Instant;

const FILE_PATH: &str =
    "s3://warehouse/default/xlarge_table/data/00000-0-3a2b40d3-cbd3-47fd-905e-a5a1033561a6.parquet";
const BUCKET: &str = "warehouse";
const KEY: &str =
    "default/xlarge_table/data/00000-0-3a2b40d3-cbd3-47fd-905e-a5a1033561a6.parquet";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // -----------------------------------------------------------------------
    // 1. Raw bytes read with opendal
    // -----------------------------------------------------------------------
    {
        use opendal::Operator;
        use opendal::services::S3;

        let builder = S3::default()
            .bucket(BUCKET)
            .endpoint("http://127.0.0.1:9000")
            .region("us-east-1")
            .access_key_id("admin")
            .secret_access_key("password");

        let op = Operator::new(builder)?.finish();

        // Warm up
        let _ = op.stat(KEY).await?;

        let t0 = Instant::now();
        let data = op.read(KEY).await?.to_bytes();
        println!(
            "opendal raw read:       {:?}  ({:.1} MB)",
            t0.elapsed(),
            data.len() as f64 / 1024.0 / 1024.0
        );
    }

    // -----------------------------------------------------------------------
    // 2. Raw bytes read with object_store
    // -----------------------------------------------------------------------
    {
        use object_store::aws::AmazonS3Builder;
        use object_store::ObjectStore;

        let store = AmazonS3Builder::new()
            .with_bucket_name(BUCKET)
            .with_endpoint("http://127.0.0.1:9000")
            .with_region("us-east-1")
            .with_access_key_id("admin")
            .with_secret_access_key("password")
            .with_allow_http(true)
            .build()?;

        let path = object_store::path::Path::from(KEY);

        // Warm up
        let _ = store.head(&path).await?;

        let t0 = Instant::now();
        let data = store.get(&path).await?.bytes().await?;
        println!(
            "object_store raw read:  {:?}  ({:.1} MB)",
            t0.elapsed(),
            data.len() as f64 / 1024.0 / 1024.0
        );
    }

    // -----------------------------------------------------------------------
    // 3. Parquet decode with object_store (Arrow RecordBatches)
    // -----------------------------------------------------------------------
    {
        use object_store::aws::AmazonS3Builder;
        use object_store::ObjectStore;
        use parquet::arrow::ParquetRecordBatchStreamBuilder;
        use parquet::arrow::async_reader::ParquetObjectReader;
        use futures::TryStreamExt;

        let store = AmazonS3Builder::new()
            .with_bucket_name(BUCKET)
            .with_endpoint("http://127.0.0.1:9000")
            .with_region("us-east-1")
            .with_access_key_id("admin")
            .with_secret_access_key("password")
            .with_allow_http(true)
            .build()?;

        let store = std::sync::Arc::new(store);
        let path = object_store::path::Path::from(KEY);
        let meta = store.head(&path).await?;
        let file_size = meta.size;

        let t0 = Instant::now();
        let reader = ParquetObjectReader::new(store.clone(), path).with_file_size(file_size);
        let stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await?
            .build()?;
        let batches: Vec<_> = stream.try_collect().await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!(
            "object_store + parquet: {:?}  ({} rows, {} batches)",
            t0.elapsed(),
            rows,
            batches.len()
        );
    }

    // -----------------------------------------------------------------------
    // 4. Parquet decode via iceberg-rust (opendal under the hood)
    // -----------------------------------------------------------------------
    {
        use futures::TryStreamExt;
        use iceberg::{Catalog, CatalogBuilder};
        use iceberg_catalog_rest::RestCatalogBuilder;
        use iceberg_storage_opendal::OpenDalStorageFactory;
        use std::sync::Arc;

        let mut props = HashMap::new();
        props.insert("uri".into(), "http://127.0.0.1:8181".into());
        props.insert("s3.endpoint".into(), "http://127.0.0.1:9000".into());
        props.insert("s3.region".into(), "us-east-1".into());
        props.insert("s3.access-key-id".into(), "admin".into());
        props.insert("s3.secret-access-key".into(), "password".into());

        let storage: Arc<dyn iceberg::io::StorageFactory> =
            Arc::new(OpenDalStorageFactory::S3 {
                configured_scheme: "s3".to_string(),
                customized_credential_load: None,
            });

        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(storage)
            .load("rest", props)
            .await?;

        let ident = iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new("default".into()),
            "xlarge_table".into(),
        );
        let table = catalog.load_table(&ident).await?;

        let t0 = Instant::now();
        let scan = table.scan().build()?;
        let stream = scan.to_arrow().await?;
        let batches: Vec<_> = stream.try_collect().await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!(
            "iceberg + opendal:     {:?}  ({} rows, {} batches)",
            t0.elapsed(),
            rows,
            batches.len()
        );
    }

    Ok(())
}
