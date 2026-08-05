#![cfg(feature = "datafusion")]
//! Integration tests for Parquet encryption via `delta.encryption.*` table properties.
//!
//! Tests are split across branches:
//!   - enc-write-path: factory registry + physical-encryption verification (no read-back)
//!   - enc-read-path:  full round-trip read/write, optimize, DML

use arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake_core::DeltaResult;
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::operations::write::encryption::register_encryption_factory;
use deltalake_core::test_utils::kms_encryption::MockKmsFactory;
use std::sync::Arc;
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;

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

/// Register a fresh factory with a unique ID to prevent test interference.
fn register_fresh_factory() -> String {
    let kms_id = format!("test-kms-{}", Uuid::new_v4());
    register_encryption_factory(&kms_id, Arc::new(MockKmsFactory::new()));
    kms_id
}

fn table_url(uri: &str) -> Url {
    Url::parse(&format!("file://{}", uri)).unwrap()
}

async fn create_encrypted_table(uri: &str, kms_id: &str) -> DeltaResult<()> {
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_table_name("test")
        .with_property("delta.encryption.kms.id", kms_id)
        .with_property("delta.encryption.footer.key", "test-footer-key")
        .await?;

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let batch = get_table_batches();
    let table = table.write(vec![batch.clone()]).await?;
    table.write(vec![batch]).await?;
    Ok(())
}

/// Walk `dir` and assert every `.parquet` file has an encrypted footer.
async fn assert_all_parquets_encrypted(dir: &std::path::Path) {
    use object_store::{ObjectStoreExt as _, local::LocalFileSystem, path::Path};
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::async_reader::ParquetObjectReader;

    fn find_parquet(d: &std::path::Path, result: &mut Vec<std::path::PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(d) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    find_parquet(&path, result);
                } else if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                    result.push(path);
                }
            }
        }
    }

    let mut parquet_files = vec![];
    find_parquet(dir, &mut parquet_files);
    assert!(
        !parquet_files.is_empty(),
        "No parquet files found — cannot verify encryption"
    );

    let store = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    for abs_path in &parquet_files {
        let rel = abs_path.strip_prefix(dir).unwrap();
        let object_path = Path::parse(rel.to_string_lossy().as_ref()).unwrap();
        let meta = store.head(&object_path).await.unwrap();
        let reader =
            ParquetObjectReader::new(store.clone(), object_path.clone()).with_file_size(meta.size);
        let result = ParquetRecordBatchStreamBuilder::new(reader).await;
        assert!(
            result.is_err(),
            "File {:?} opened without decryption — NOT encrypted!",
            rel
        );
    }
}

// ---------------------------------------------------------------------------
// Tests available at the enc-write-path stage
// (no DataFusion read-back required)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_missing_factory_returns_error() {
    use deltalake_core::operations::write::encryption::get_encryption_factory;

    let impossible_kms = format!("never-registered-{}", Uuid::new_v4());
    assert!(
        get_encryption_factory(&impossible_kms).is_none(),
        "Factory should not be registered"
    );
}

/// Critical correctness test: verify files are physically encrypted on disk.
/// Opens each parquet file with the raw reader (no decryption) and asserts
/// the read fails, proving the encryption was applied to the footer.
#[tokio::test]
async fn test_parquet_files_are_physically_encrypted() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    create_encrypted_table(dir.path().to_str().unwrap(), &kms_id).await?;
    assert_all_parquets_encrypted(dir.path()).await;
    Ok(())
}

/// The advertised precedence guarantee: caller-supplied `WriterProperties`
/// must not defeat table encryption.
#[tokio::test]
async fn test_caller_writer_properties_cannot_defeat_encryption() -> DeltaResult<()> {
    use parquet::file::properties::WriterProperties;

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, &kms_id).await?;

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    table
        .write(vec![get_table_batches()])
        .with_writer_properties(WriterProperties::builder().build())
        .await?;

    assert_all_parquets_encrypted(dir.path()).await;
    Ok(())
}

/// The legacy `RecordBatchWriter` must resolve encryption from the table
/// configuration (via the global registry) rather than writing plaintext.
#[tokio::test]
async fn test_legacy_record_batch_writer_is_encrypted() -> DeltaResult<()> {
    use deltalake_core::writer::{DeltaWriter as _, RecordBatchWriter};

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, &kms_id).await?;

    let mut table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let mut writer = RecordBatchWriter::for_table(&table)?;
    writer.write(get_table_batches()).await?;
    writer.flush_and_commit(&mut table).await?;

    assert_all_parquets_encrypted(dir.path()).await;
    Ok(())
}

/// A DataFusion `INSERT INTO` through the table provider's DataSink must
/// encrypt like every other write path.
#[tokio::test]
async fn test_insert_into_datasink_is_encrypted() -> DeltaResult<()> {
    use datafusion::prelude::SessionContext;

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, &kms_id).await?;

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let ctx = SessionContext::new();
    ctx.register_table("t", table.table_provider().await?)?;
    ctx.sql("INSERT INTO t (int, string) VALUES (42, 'Z')")
        .await?
        .collect()
        .await?;

    assert_all_parquets_encrypted(dir.path()).await;
    Ok(())
}

/// A single operation that creates the table (with encryption in its
/// configuration) and writes data must encrypt that first commit's files too.
#[tokio::test]
async fn test_create_with_data_in_one_call_is_encrypted() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    table
        .write(vec![get_table_batches()])
        .with_configuration(vec![
            ("delta.encryption.kms.id".to_string(), Some(kms_id.clone())),
            (
                "delta.encryption.footer.key".to_string(),
                Some("test-footer-key".to_string()),
            ),
        ])
        .await?;

    assert_all_parquets_encrypted(dir.path()).await;
    Ok(())
}

/// A partially-configured table (either encryption property alone) must fail
/// writes instead of silently writing plaintext.
#[tokio::test]
async fn test_partially_configured_encryption_errors_on_write() -> DeltaResult<()> {
    for props in [
        vec![("delta.encryption.kms.id", "some-kms")],
        vec![("delta.encryption.footer.key", "some-key")],
    ] {
        let dir = TempDir::new()?;
        let uri = dir.path().to_str().unwrap();
        let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
        let mut create = table.create().with_columns(get_table_columns());
        for (k, v) in &props {
            create = create.with_property(*k, *v);
        }
        create.await?;

        let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
            .load()
            .await?;
        let result = table.write(vec![get_table_batches()]).await;
        assert!(
            result.is_err(),
            "write on a partially-configured table ({props:?}) must error, not write plaintext"
        );
    }
    Ok(())
}

/// The descriptive unregistered-factory error must surface through an actual
/// write, not just the registry lookup.
#[tokio::test]
async fn test_unregistered_factory_errors_on_write() -> DeltaResult<()> {
    let unregistered = format!("never-registered-{}", Uuid::new_v4());
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_property("delta.encryption.kms.id", unregistered.as_str())
        .with_property("delta.encryption.footer.key", "test-footer-key")
        .await?;

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let err = table
        .write(vec![get_table_batches()])
        .await
        .expect_err("write without a registered factory must fail");
    assert!(
        err.to_string().contains("No EncryptionFactory registered"),
        "expected the descriptive registry error, got: {err}"
    );
    Ok(())
}
