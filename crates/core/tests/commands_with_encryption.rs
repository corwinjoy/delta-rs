//! Integration tests for Parquet encryption via `delta.encryption.*` table properties.
//!
//! Encryption is configured by setting Delta table properties at table creation time.
//! A factory is registered globally once and all operations (write, read, delete, update,
//! merge, optimize) automatically encrypt/decrypt without any per-operation configuration.

use arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::{
    assert_batches_sorted_eq,
    logical_expr::{col, lit},
    prelude::SessionContext,
};
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::operations::optimize::OptimizeType;
use deltalake_core::operations::write::encryption::register_encryption_factory;
use deltalake_core::test_utils::kms_encryption::MockKmsFactory;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableError};
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;

const TEST_KMS_ID: &str = "test-kms-for-integration-tests";

fn get_table_columns() -> Vec<StructField> {
    vec![
        StructField::new("int", DataType::Primitive(PrimitiveType::Integer), false),
        StructField::new("string", DataType::Primitive(PrimitiveType::String), true),
        StructField::new(
            "timestamp",
            DataType::Primitive(PrimitiveType::TimestampNtz),
            true,
        ),
    ]
}

fn get_table_batches() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("int", ArrowDataType::Int32, false),
        Field::new("string", ArrowDataType::Utf8, true),
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    let int_vals = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let str_vals = StringArray::from(vec!["A", "B", "C", "B", "A", "C", "A", "B", "B", "A", "A"]);
    let ts_vals = TimestampMicrosecondArray::from(vec![
        1000000012, 1000000012, 1000000012, 1000000012, 500012305, 500012305, 500012305, 500012305,
        500012305, 500012305, 500012305,
    ]);
    RecordBatch::try_new(
        schema,
        vec![Arc::new(int_vals), Arc::new(str_vals), Arc::new(ts_vals)],
    )
    .unwrap()
}

fn ensure_factory_registered() -> Arc<MockKmsFactory> {
    let factory = Arc::new(MockKmsFactory::new());
    register_encryption_factory(TEST_KMS_ID, factory.clone());
    factory
}

fn table_url(uri: &str) -> Url {
    Url::parse(&format!("file://{}", uri)).unwrap()
}

async fn create_encrypted_table(uri: &str, table_name: &str) -> DeltaResult<DeltaTable> {
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_table_name(table_name)
        .with_property("delta.encryption.kms.id", TEST_KMS_ID)
        .with_property("delta.encryption.footer.key", "test-footer-key")
        .await?;

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let batch = get_table_batches();
    let table = table.write(vec![batch.clone()]).await?;
    let table = table.write(vec![batch.clone()]).await?;
    Ok(table)
}

async fn read_table(uri: &str) -> DeltaResult<Vec<RecordBatch>> {
    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(table.scan_table().build().await?))?;
    let batches = ctx.sql("SELECT * FROM t").await?.collect().await?;
    Ok(batches)
}

#[tokio::test]
async fn test_encrypted_create_and_read() -> DeltaResult<()> {
    ensure_factory_registered();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test").await?;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_encrypted_optimize_compact() -> DeltaResult<()> {
    ensure_factory_registered();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test").await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table.optimize().await?;
    assert!(
        metrics.num_files_added > 0 || metrics.num_files_removed > 0,
        "Compact should have changed files: {metrics:?}"
    );
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_encrypted_optimize_zorder() -> DeltaResult<()> {
    ensure_factory_registered();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test").await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["int".to_string()]))
        .await?;
    assert!(metrics.num_files_added > 0, "Z-order should add files");
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_encrypted_delete() -> DeltaResult<()> {
    ensure_factory_registered();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test").await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table.delete().with_predicate(col("int").eq(lit(1))).await?;
    assert!(metrics.num_deleted_rows > 0);
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_encrypted_update() -> DeltaResult<()> {
    ensure_factory_registered();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test").await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table
        .update()
        .with_predicate(col("int").eq(lit(1)))
        .with_update("int", lit(100))
        .await?;
    assert!(metrics.num_updated_rows > 0);
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}
