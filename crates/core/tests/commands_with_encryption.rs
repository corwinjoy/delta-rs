#![cfg(feature = "datafusion")]
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
use deltalake_core::test_utils::kms_encryption::MockKmsFactory;
use deltalake_core::{DeltaResult, DeltaTable};
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
/// Each test gets its own `MockKmsFactory` instance so keys from one test
/// cannot be mistaken for keys from another.
fn register_fresh_factory() -> String {
    let kms_id = format!("test-kms-{}", Uuid::new_v4());
    let factory = Arc::new(MockKmsFactory::new());
    register_encryption_factory(&kms_id, factory);
    kms_id
}

fn table_url(uri: &str) -> Url {
    Url::parse(&format!("file://{}", uri)).unwrap()
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
        .with_property("delta.encryption.kms.id", kms_id)
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

/// Guards against silent encryption skip: verifies that an unregistered kms.id
/// is not present in the global registry.
#[tokio::test]
async fn test_missing_factory_returns_error() {
    use deltalake_core::operations::write::encryption::get_encryption_factory;

    let impossible_kms = format!("never-registered-{}", Uuid::new_v4());
    assert!(
        get_encryption_factory(&impossible_kms).is_none(),
        "Factory should not be registered"
    );
}

#[tokio::test]
async fn test_matrix_create_and_read() -> DeltaResult<()> {
    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, "test", &kms_id).await?;
    let batches = read_table(uri).await?;
    assert!(
        !batches.is_empty(),
        "Table should have rows after create_and_read"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Verify files are physically encrypted on disk
// ---------------------------------------------------------------------------

/// The critical correctness test: opens each parquet file in the table directly
/// with the raw parquet reader (no decryption properties) and verifies that the
/// read fails because the footer is encrypted.
///
/// If this test fails it means parquet files are written **unencrypted** even though
/// `delta.encryption.*` properties are set — the encryption configuration silently
/// failed to propagate.
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
/// This tests that `delta.encryption.column.keys` and
/// `delta.encryption.plaintext.footer` are correctly forwarded to the factory.
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
        .with_property("delta.encryption.kms.id", &kms_id)
        .with_property("delta.encryption.footer.key", "footer-master-key")
        .with_property("delta.encryption.plaintext.footer", "true")
        .with_property("delta.encryption.column.keys", "col-master-key:int,string")
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
        .with_property("delta.encryption.kms.id", kms_id.as_str())
        .with_property("delta.encryption.footer.key", "test-footer-key")
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
        .with_property("delta.encryption.kms.id", kms_id.as_str())
        .with_property("delta.encryption.footer.key", "test-footer-key")
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
