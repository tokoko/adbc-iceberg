//! Direct Rust scan benchmark — no ADBC, no C ABI, no Python.
//! Measures iceberg-rust + parquet-rs performance directly.
//!
//! Usage:
//!     cargo run --release --example direct_scan

use futures::TryStreamExt;
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut props = HashMap::new();
    props.insert("uri".into(), "http://127.0.0.1:8181".into());
    props.insert("s3.endpoint".into(), "http://127.0.0.1:9000".into());
    props.insert("s3.region".into(), "us-east-1".into());
    props.insert("s3.access-key-id".into(), "admin".into());
    props.insert("s3.secret-access-key".into(), "password".into());

    let storage: Arc<dyn iceberg::io::StorageFactory> = Arc::new(OpenDalStorageFactory::S3 {
        configured_scheme: "s3".to_string(),
        customized_credential_load: None,
    });

    let t0 = Instant::now();
    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(storage)
        .load("rest", props)
        .await?;
    println!("Catalog init: {:?}", t0.elapsed());

    let ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new("default".into()),
        "xlarge_table".into(),
    );

    let t0 = Instant::now();
    let table = catalog.load_table(&ident).await?;
    println!("Load table:   {:?}", t0.elapsed());

    // --- Full scan ---
    let t0 = Instant::now();
    let scan = table.scan().build()?;
    let stream = scan.to_arrow().await?;
    let batches: Vec<_> = stream.try_collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("\nFull scan:    {:?}  ({} rows, {} batches)", t0.elapsed(), rows, batches.len());

    // --- Column projection ---
    let t0 = Instant::now();
    let scan = table.scan().select(["id", "name"]).build()?;
    let stream = scan.to_arrow().await?;
    let batches: Vec<_> = stream.try_collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("Projection:   {:?}  ({} rows)", t0.elapsed(), rows);

    // --- Predicate: id < 100 ---
    let t0 = Instant::now();
    let scan = table
        .scan()
        .with_filter(iceberg::expr::Reference::new("id").less_than(iceberg::spec::Datum::long(100)))
        .build()?;
    let stream = scan.to_arrow().await?;
    let batches: Vec<_> = stream.try_collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("id < 100:     {:?}  ({} rows)", t0.elapsed(), rows);

    // --- Predicate: name = 'alice' ---
    let t0 = Instant::now();
    let scan = table
        .scan()
        .with_filter(
            iceberg::expr::Reference::new("name")
                .equal_to(iceberg::spec::Datum::string("alice")),
        )
        .build()?;
    let stream = scan.to_arrow().await?;
    let batches: Vec<_> = stream.try_collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("name=alice:   {:?}  ({} rows)", t0.elapsed(), rows);

    // --- Projection + predicate ---
    let t0 = Instant::now();
    let scan = table
        .scan()
        .select(["id", "value"])
        .with_filter(
            iceberg::expr::Reference::new("id")
                .less_than(iceberg::spec::Datum::long(1000)),
        )
        .build()?;
    let stream = scan.to_arrow().await?;
    let batches: Vec<_> = stream.try_collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!("proj+pred:    {:?}  ({} rows)", t0.elapsed(), rows);

    Ok(())
}
