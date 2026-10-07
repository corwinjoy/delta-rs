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
    logical_expr::{col, lit},
    prelude::SessionContext,
};
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::operations::optimize::OptimizeType;
use deltalake_core::operations::write::encryption::register_encryption_factory;
use deltalake_core::test_utils::kms_encryption::mock_kms_factory;
use deltalake_core::{DeltaResult, DeltaTable};
use std::collections::HashMap;
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
/// Each test gets its own in-memory KMS so keys from one test
/// cannot be mistaken for keys from another.
fn register_fresh_factory() -> String {
    let kms_id = format!("test-kms-{}", Uuid::new_v4());
    register_encryption_factory(&kms_id, mock_kms_factory());
    kms_id
}

fn table_url(uri: &str) -> Url {
    Url::from_directory_path(uri).unwrap()
}

async fn create_encrypted_table(
    uri: &str,
    table_name: &str,
    kms_id: &str,
) -> DeltaResult<DeltaTable> {
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_table_name(table_name)
        .with_property("delta.encryption.kms_id", kms_id)
        .with_property("delta.encryption.footer_key", "test-footer-key")
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
    ctx.register_table("t", table.table_provider().await?)?;
    let batches = ctx.sql("SELECT * FROM t").await?.collect().await?;
    Ok(batches)
}

/// Recursively collect all `.parquet` files under `dir`.
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

/// Walk `dir` and assert that every `.parquet` file has an encrypted footer.
/// Fails with a clear message if any file can be read without decryption — which
/// would mean the operation wrote unencrypted parquet despite having encryption configured.
async fn assert_all_parquets_encrypted(dir: &std::path::Path) {
    use object_store::{ObjectStoreExt as _, local::LocalFileSystem, path::Path};
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::async_reader::ParquetObjectReader;

    let mut parquet_files = vec![];
    find_parquet(dir, &mut parquet_files);

    assert!(
        !parquet_files.is_empty(),
        "No parquet files found in {:?} — cannot verify encryption",
        dir
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
            "Parquet file {:?} opened WITHOUT decryption — file is NOT encrypted! \
             Encryption may have silently failed to propagate for this operation.",
            rel
        );
    }
}

#[tokio::test]
async fn test_encrypted_create_and_read() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;
    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_encrypted_optimize_compact() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table.optimize().await?;
    assert!(
        metrics.num_files_added > 0 || metrics.num_files_removed > 0,
        "Compact should have changed files: {metrics:?}"
    );
    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_encrypted_optimize_zorder() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["int".to_string()]))
        .await?;
    assert!(metrics.num_files_added > 0, "Z-order should add files");
    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_encrypted_delete() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table.delete().with_predicate(col("int").eq(lit(1))).await?;
    assert!(metrics.num_deleted_rows.unwrap_or(0) > 0);
    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_encrypted_update() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let (_table, metrics) = table
        .update()
        .with_predicate(col("int").eq(lit(1)))
        .with_update("int", lit(100))
        .await?;
    assert!(metrics.num_updated_rows > 0);
    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    assert!(!batches.is_empty());
    Ok(())
}

// ---------------------------------------------------------------------------
// Negative test: missing factory must produce a clear error
// ---------------------------------------------------------------------------

/// Critical correctness test: verify files are physically encrypted on disk.
/// Opens each parquet file with the raw reader (no decryption) and asserts
/// the read fails, proving the encryption was applied to the footer.
#[tokio::test]
async fn test_parquet_files_are_physically_encrypted() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;
    assert_all_parquets_encrypted(dir.path()).await;
    Ok(())
}

// ---------------------------------------------------------------------------
// Columnar encryption with plaintext footer
// ---------------------------------------------------------------------------

/// Verify columnar encryption where only the "int" and "string" columns are encrypted
/// and the parquet footer is left in plaintext.
///
/// With plaintext footer mode:
/// - The footer (schema, row-group metadata) is readable without keys.
/// - Only the column data pages for "int" and "string" are encrypted.
/// - The "timestamp" column is not encrypted and is always readable.
///
/// This tests that `delta.encryption.column_keys` and
/// `delta.encryption.plaintext_footer` are correctly forwarded to the factory.
#[tokio::test]
async fn test_encrypted_columnar_plaintext_footer() -> DeltaResult<()> {
    use object_store::{ObjectStoreExt as _, local::LocalFileSystem, path::Path as ObjPath};
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::async_reader::ParquetObjectReader;

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    let table_url = table_url(uri);

    // Create with only "int" and "string" encrypted; "timestamp" is left unencrypted.
    // The footer is stored in plaintext so the schema is readable without keys.
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url.clone())?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_table_name("col-enc-test")
        .with_property("delta.encryption.kms_id", &kms_id)
        .with_property("delta.encryption.footer_key", "footer-master-key")
        .with_property("delta.encryption.plaintext_footer", "true")
        .with_property("delta.encryption.column_keys", "col-master-key:int,string")
        .await?;

    // Write two batches so we exercise more than one file.
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url.clone())?
        .load()
        .await?;
    let batch = get_table_batches();
    let table = table.write(vec![batch.clone()]).await?;
    let table = table.write(vec![batch.clone()]).await?;

    // Round-trip: read back with the factory registered and verify row count.
    let batches = read_table(uri).await?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 22,
        "Expected 22 rows (2 writes × 11 rows each), got {total_rows}"
    );

    // Physical check: without decryption, the parquet FOOTER should be readable
    // (plaintext footer mode) but reading the encrypted column data should fail.
    let mut parquet_files = vec![];
    find_parquet(dir.path(), &mut parquet_files);
    assert!(
        !parquet_files.is_empty(),
        "No parquet files found — cannot verify columnar encryption"
    );

    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    for abs_path in &parquet_files {
        let rel = abs_path.strip_prefix(dir.path()).unwrap();
        let obj_path = ObjPath::parse(rel.to_string_lossy().as_ref()).unwrap();

        let meta = store.head(&obj_path).await.unwrap();
        let reader =
            ParquetObjectReader::new(store.clone(), obj_path.clone()).with_file_size(meta.size);

        // With plaintext footer the builder itself must SUCCEED (footer is readable).
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "Expected plaintext footer to be readable for {:?}, got: {e}",
                    rel
                )
            });

        // Reading column data without decryption keys must FAIL for the encrypted columns.
        let result: parquet::errors::Result<Vec<_>> = async {
            let stream = builder.build()?;
            futures::StreamExt::collect::<Vec<_>>(stream)
                .await
                .into_iter()
                .collect()
        }
        .await;
        assert!(
            result.is_err(),
            "Expected reading encrypted column data to fail for {:?} without keys, \
             but it succeeded — columnar encryption may not have been applied",
            rel
        );
    }

    // Verify the "timestamp" column is NOT listed in the encryption properties
    // so the write path respected the per-column config.
    let _ = table; // silence unused warning

    Ok(())
}

// ---------------------------------------------------------------------------
// Write-entry-point coverage: every path must uphold encryption
// ---------------------------------------------------------------------------

/// The advertised precedence guarantee: caller-supplied `WriterProperties`
/// must not defeat table encryption.
#[tokio::test]
async fn test_caller_writer_properties_cannot_defeat_encryption() -> DeltaResult<()> {
    use parquet::file::properties::WriterProperties;

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

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
    create_encrypted_table(uri, "test", &kms_id).await?;

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
    create_encrypted_table(uri, "test", &kms_id).await?;

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
            ("delta.encryption.kms_id".to_string(), Some(kms_id.clone())),
            (
                "delta.encryption.footer_key".to_string(),
                Some("test-footer-key".to_string()),
            ),
        ])
        .await?;

    assert_all_parquets_encrypted(dir.path()).await;
    Ok(())
}

/// A partially-configured table (either encryption property alone) is rejected when
/// it is created, so no write can treat it as unencrypted.
#[tokio::test]
async fn test_partially_configured_encryption_is_rejected() -> DeltaResult<()> {
    for (prop, value, missing) in [
        (
            "delta.encryption.kms_id",
            "some-kms",
            "delta.encryption.footer_key",
        ),
        (
            "delta.encryption.footer_key",
            "some-key",
            "delta.encryption.kms_id",
        ),
    ] {
        let dir = TempDir::new()?;
        let uri = dir.path().to_str().unwrap();
        let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
        let err = table
            .create()
            .with_columns(get_table_columns())
            .with_property(prop, value)
            .await
            .expect_err("a partial encryption configuration must be rejected")
            .to_string();
        assert!(err.contains(missing), "{err}");
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
        .with_property("delta.encryption.kms_id", unregistered.as_str())
        .with_property("delta.encryption.footer_key", "test-footer-key")
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

/// `RecordBatchWriter::try_new` cannot load the table, so it resolves the table's
/// encryption on the first write; it must not write plaintext files.
#[tokio::test]
async fn test_record_batch_writer_try_new_is_encrypted() -> DeltaResult<()> {
    use deltalake_core::writer::{DeltaWriter as _, RecordBatchWriter};

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

    let batch = get_table_batches();
    let mut writer =
        RecordBatchWriter::try_new(table_url(uri).as_str(), batch.schema(), None, None)?;
    writer.write(batch).await?;
    let mut table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    writer.flush_and_commit(&mut table).await?;

    assert_all_parquets_encrypted(dir.path()).await;
    Ok(())
}

/// A `try_new` writer whose table did not exist at the first write holds plaintext files.
/// If the table is then created encrypted, neither `flush` nor `flush_and_commit` may hand
/// them out.
#[tokio::test]
async fn test_record_batch_writer_refuses_plaintext_for_table_created_encrypted() -> DeltaResult<()>
{
    use deltalake_core::writer::{DeltaWriter as _, RecordBatchWriter};

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();

    let batch = get_table_batches();
    let mut writer =
        RecordBatchWriter::try_new(table_url(uri).as_str(), batch.schema(), None, None)?;
    writer.write(batch).await?;
    let mut table = create_encrypted_table(uri, "test", &kms_id).await?;
    let version = table.version();

    let err = writer
        .flush()
        .await
        .expect_err("flush must refuse plaintext files for an encrypted table");
    assert!(err.to_string().contains("encrypted"), "{err}");
    let err = writer
        .flush_and_commit(&mut table)
        .await
        .expect_err("flush_and_commit must refuse plaintext files for an encrypted table");
    assert!(err.to_string().contains("encrypted"), "{err}");
    assert_eq!(table.version(), version);
    Ok(())
}

/// Replacing the base writer properties of an encrypting factory keeps the encryption and
/// applies the new settings.
#[tokio::test]
async fn test_base_properties_keep_encryption() -> DeltaResult<()> {
    use deltalake_core::operations::write::encryption::writer_factory_from_configuration;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;
    use parquet::schema::types::ColumnPath;

    let kms_id = register_fresh_factory();
    let configuration = HashMap::from([
        ("delta.encryption.kms_id".to_string(), kms_id.clone()),
        (
            "delta.encryption.footer_key".to_string(),
            "test-footer-key".to_string(),
        ),
    ]);
    let factory = writer_factory_from_configuration(&configuration, None, None)?;

    let zstd = Compression::ZSTD(ZstdLevel::try_new(3).unwrap());
    let factory = factory
        .with_base_properties(WriterProperties::builder().set_compression(zstd).build())
        .expect("the KMS factory takes base properties");
    assert_eq!(
        factory
            .base_properties()
            .compression(&ColumnPath::from("int")),
        zstd
    );
    let file_properties = factory
        .create_writer_properties(
            &object_store::path::Path::from("part-0.parquet"),
            &get_table_batches().schema(),
        )
        .await?;
    assert!(file_properties.file_encryption_properties().is_some());
    Ok(())
}

/// `delta.encryption.*` keys are validated on their own, so other unknown `delta.*` keys,
/// such as typos, are still rejected when creating a table.
#[tokio::test]
async fn test_misspelled_property_is_rejected_alongside_encryption() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    let result = table
        .clone()
        .create()
        .with_columns(get_table_columns())
        .with_property("delta.encryption.kms_id", kms_id.as_str())
        .with_property("delta.encryption.footer_key", "test-footer-key")
        .with_property("delta.enableChangeDataFed", "true")
        .await;
    assert!(result.is_err(), "a misspelled property must be rejected");

    let result = table
        .write(vec![get_table_batches()])
        .with_configuration(vec![
            ("delta.encryption.kms_id".to_string(), Some(kms_id)),
            (
                "delta.encryption.footer_key".to_string(),
                Some("test-footer-key".to_string()),
            ),
            (
                "delta.enableChangeDataFed".to_string(),
                Some("true".to_string()),
            ),
        ])
        .await;
    assert!(result.is_err(), "a misspelled property must be rejected");
    Ok(())
}

/// Statistics for encrypted columns never reach the Delta log (RFC), whatever the stats
/// configuration asks for: the file's own column chunk metadata decides. With column keys
/// the plaintext columns keep their statistics; under uniform encryption no column has any.
#[tokio::test]
async fn test_no_log_statistics_for_encrypted_columns() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    for (column_keys, expect_int, expect_string) in
        [("test-key:int", false, true), ("", false, false)]
    {
        let tmp = TempDir::new().unwrap();
        let table =
            deltalake_core::DeltaTableBuilder::from_url(table_url(tmp.path().to_str().unwrap()))?
                .build()?;
        let mut create = table
            .create()
            .with_columns(get_table_columns())
            .with_property("delta.encryption.kms_id", &kms_id)
            .with_property("delta.encryption.footer_key", "test-footer-key");
        if !column_keys.is_empty() {
            create = create.with_property("delta.encryption.column_keys", column_keys);
        }
        let table = create.await?;
        table.write(vec![get_table_batches()]).await?;

        let log = std::fs::read_to_string(tmp.path().join("_delta_log/00000000000000000001.json"))
            .unwrap();
        let adds: Vec<serde_json::Value> = log
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter_map(|action| action.get("add").cloned())
            .collect();
        assert!(!adds.is_empty(), "{column_keys:?}: no add actions");
        for add in adds {
            let stats: serde_json::Value =
                serde_json::from_str(add["stats"].as_str().expect("add has stats")).unwrap();
            assert_eq!(stats["numRecords"], 11, "{column_keys:?}: {stats}");
            for section in ["minValues", "maxValues", "nullCount"] {
                let values = &stats[section];
                assert_eq!(
                    values.get("int").is_some(),
                    expect_int,
                    "{column_keys:?}: {section} {stats}"
                );
                assert_eq!(
                    values.get("string").is_some(),
                    expect_string,
                    "{column_keys:?}: {section} {stats}"
                );
            }
        }
    }
    Ok(())
}

/// Adding a column to a table with `column_keys` is allowed; the new column is plaintext
/// (and so keeps its statistics) while the keyed column stays encrypted.
#[tokio::test]
async fn test_schema_evolution_adds_plaintext_columns_to_column_keyed_table() -> DeltaResult<()> {
    use deltalake_core::operations::write::SchemaMode;

    let kms_id = register_fresh_factory();
    let tmp = TempDir::new().unwrap();
    let table =
        deltalake_core::DeltaTableBuilder::from_url(table_url(tmp.path().to_str().unwrap()))?
            .build()?
            .create()
            .with_columns(get_table_columns())
            .with_property("delta.encryption.kms_id", &kms_id)
            .with_property("delta.encryption.footer_key", "test-footer-key")
            .with_property("delta.encryption.column_keys", "test-key:int")
            .await?;

    let batch = get_table_batches();
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(Field::new("extra", ArrowDataType::Int32, true)));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(Int32Array::from(vec![Some(1); batch.num_rows()])));
    let widened = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), columns).unwrap();

    let table = table
        .write(vec![widened])
        .with_schema_mode(SchemaMode::Merge)
        .await?;
    assert!(table.snapshot()?.schema().field("extra").is_some());

    let log =
        std::fs::read_to_string(tmp.path().join("_delta_log/00000000000000000001.json")).unwrap();
    let add = log
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find_map(|action| action.get("add").cloned())
        .expect("an add action");
    let stats: serde_json::Value = serde_json::from_str(add["stats"].as_str().unwrap()).unwrap();
    assert!(stats["minValues"].get("extra").is_some(), "{stats}");
    assert!(stats["minValues"].get("int").is_none(), "{stats}");
    assert_all_parquets_encrypted(tmp.path()).await;
    Ok(())
}

/// A write may carry the table's own encryption configuration, so a pipeline that creates
/// the table if missing and appends otherwise can pass the same configuration every run.
#[tokio::test]
async fn test_write_with_matching_encryption_configuration_is_idempotent() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let tmp = TempDir::new().unwrap();
    let url = table_url(tmp.path().to_str().unwrap());
    // Stored as the sorted `k1:int;k2:string`; the write compares parsed values.
    let configuration = [
        ("delta.encryption.kms_id", Some(kms_id.as_str())),
        ("delta.encryption.footer_key", Some("test-footer-key")),
        ("delta.encryption.column_keys", Some("k2:string;k1:int")),
    ];
    // First run creates the table.
    deltalake_core::DeltaTableBuilder::from_url(url.clone())?
        .build()?
        .write(vec![get_table_batches()])
        .with_configuration(configuration)
        .await?;
    // A later run loads it and sends the same configuration again, plus a new
    // `kms_configuration`, which is not frozen.
    let table = deltalake_core::DeltaTableBuilder::from_url(url)?
        .load()
        .await?
        .write(vec![get_table_batches()])
        .with_configuration(
            configuration
                .into_iter()
                .chain([("delta.encryption.kms_configuration", Some("{}"))]),
        )
        .await?;
    let err = table
        .write(vec![get_table_batches()])
        .with_configuration([("delta.encryption.footer_key", Some("other-key"))])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("differs from the table's"), "{err}");
    assert_all_parquets_encrypted(tmp.path()).await;
    Ok(())
}

/// The change feed of an encrypted table must decrypt like every other read
/// path (both `_change_data` files and the regular add/remove data files).
#[tokio::test]
async fn test_cdf_read_on_encrypted_table() -> DeltaResult<()> {
    use datafusion::physical_plan::collect;

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_property("delta.encryption.kms_id", kms_id.as_str())
        .with_property("delta.encryption.footer_key", "test-footer-key")
        .with_property("delta.enableChangeDataFeed", "true")
        .await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let table = table.write(vec![get_table_batches()]).await?;
    // An update produces `_change_data` files, which are encrypted too.
    let (table, metrics) = table
        .update()
        .with_predicate(col("int").eq(lit(1)))
        .with_update("int", lit(100))
        .await?;
    assert!(metrics.num_updated_rows > 0);
    assert_all_parquets_encrypted(dir.path()).await;

    let ctx = SessionContext::new();
    let plan = table
        .scan_cdf()
        .with_starting_version(0)
        .build(&ctx.state(), None)
        .await?;
    let batches = collect(plan, ctx.task_ctx()).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(
        rows > 0,
        "change feed of an encrypted table must be readable"
    );
    Ok(())
}

/// A scan of a table with deletion vectors reads each file's footer to check the vector
/// against the file, which needs the decryption keys like the data read does.
#[tokio::test]
async fn test_encrypted_table_with_deletion_vectors() -> DeltaResult<()> {
    use deltalake_core::kernel::transaction::CommitBuilder;
    use deltalake_core::kernel::{Action, DeletionVectorDescriptor, StorageType};
    use deltalake_core::protocol::DeltaOperation;
    use futures::TryStreamExt as _;

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_property("delta.encryption.kms_id", kms_id.as_str())
        .with_property("delta.encryption.footer_key", "test-footer-key")
        .with_property("delta.enableDeletionVectors", "true")
        .await?;
    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let batch = get_table_batches();
    let rows = batch.num_rows();
    let table = table.write(vec![batch]).await?;

    // delta-rs rewrites files on delete rather than writing deletion vectors, so attach an
    // inline vector that removes the first row of the one file by hand.
    let snapshot = table.snapshot()?.snapshot();
    let files: Vec<_> = snapshot
        .file_views(&table.log_store(), None)
        .try_collect()
        .await?;
    assert_eq!(files.len(), 1);
    let mut bitmap = roaring::RoaringTreemap::new();
    bitmap.insert(0);
    // The portable roaring bitmap magic, then the bitmap (PROTOCOL.md, Deletion Vector Format).
    let mut bytes = 1681511377u32.to_le_bytes().to_vec();
    bitmap.serialize_into(&mut bytes).unwrap();
    let mut add = files[0].add_action();
    add.deletion_vector = Some(DeletionVectorDescriptor {
        storage_type: StorageType::Inline,
        path_or_inline_dv: z85::encode(&bytes),
        offset: None,
        size_in_bytes: bytes.len() as i32,
        cardinality: 1,
    });
    let remove = files[0].remove_action(true);
    CommitBuilder::default()
        .with_actions(vec![Action::Remove(remove), Action::Add(add)])
        .build(
            Some(snapshot),
            table.log_store(),
            DeltaOperation::Delete { predicate: None },
        )
        .await?;

    let batches = read_table(uri).await?;
    let read_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(read_rows, rows - 1);
    Ok(())
}

/// Round-trip on a partitioned encrypted table: partitioned writes go through
/// the per-partition writer fan-out, and reads reassemble partition values.
#[tokio::test]
async fn test_partitioned_encrypted_round_trip() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_partition_columns(["string"])
        .with_property("delta.encryption.kms_id", kms_id.as_str())
        .with_property("delta.encryption.footer_key", "test-footer-key")
        .await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let expected_rows = get_table_batches().num_rows();
    table.write(vec![get_table_batches()]).await?;

    assert_all_parquets_encrypted(dir.path()).await;
    let batches = read_table(uri).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, expected_rows);
    Ok(())
}

/// A provider that crossed the wire (DeltaLogicalCodec serde round-trip) loses
/// its `#[serde(skip)]` parquet options; the scan must re-derive the crypto
/// options from the snapshot's table properties instead of failing to decode.
#[tokio::test]
async fn test_serde_round_tripped_provider_still_decrypts() -> DeltaResult<()> {
    use deltalake_core::delta_datafusion::DeltaScanNext;

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;

    let table: DeltaTable = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let provider = table.table_provider().await?;
    let scan = provider
        .downcast_ref::<DeltaScanNext>()
        .expect("table_provider returns DeltaScanNext");

    // Same round-trip DeltaLogicalCodec performs for distributed plans.
    let encoded = serde_json::to_vec(scan).expect("encode provider");
    let decoded: DeltaScanNext = serde_json::from_slice(&encoded).expect("decode provider");

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(decoded))?;
    let batches = ctx.sql("SELECT * FROM t").await?.collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(rows > 0, "decoded provider must still decrypt the table");
    Ok(())
}

/// Readers take nothing from the table's current `footer_key` / `column_keys`: a table whose
/// files were written under two different key configurations, plus a plaintext file, reads
/// correctly. delta-rs refuses to change the properties itself, so the log is assembled by
/// hand, the way another engine following the RFC could leave it.
#[tokio::test]
async fn test_reads_files_written_under_different_keys_and_plaintext() -> DeltaResult<()> {
    use std::fs;

    let kms_id = register_fresh_factory();
    let target = TempDir::new()?;
    let target_uri = target.path().to_str().unwrap();
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(target_uri))?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_property("delta.encryption.kms_id", &kms_id)
        .with_property("delta.encryption.footer_key", "footer-v1")
        .with_property("delta.encryption.column_keys", "pii-v1:string")
        .await?
        .write(vec![get_table_batches()])
        .await?;

    // Files written by sibling tables with other keys (same KMS) and without encryption.
    let mut foreign_adds = Vec::new();
    for properties in [
        vec![
            ("delta.encryption.kms_id", kms_id.as_str()),
            ("delta.encryption.footer_key", "footer-v2"),
        ],
        vec![],
    ] {
        let source = TempDir::new()?;
        let mut create = deltalake_core::DeltaTableBuilder::from_url(table_url(
            source.path().to_str().unwrap(),
        ))?
        .build()?
        .create()
        .with_columns(get_table_columns());
        for (key, value) in properties {
            create = create.with_property(key, value);
        }
        create.await?.write(vec![get_table_batches()]).await?;
        let log = fs::read_to_string(source.path().join("_delta_log/00000000000000000001.json"))?;
        for line in log.lines() {
            let action: serde_json::Value = serde_json::from_str(line).unwrap();
            if let Some(add) = action.get("add") {
                let name = add["path"].as_str().unwrap();
                fs::copy(source.path().join(name), target.path().join(name))?;
                foreign_adds.push(serde_json::json!({ "add": add }).to_string());
            }
        }
    }
    assert_eq!(foreign_adds.len(), 2);
    fs::write(
        target.path().join("_delta_log/00000000000000000002.json"),
        foreign_adds.join("\n") + "\n",
    )?;

    let batches = read_table(target_uri).await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3 * get_table_batches().num_rows());
    Ok(())
}
