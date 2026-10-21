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
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
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
fn register_fresh_factory() -> String {
    let kms_id = format!("test-kms-{}", Uuid::new_v4());
    register_encryption_factory(&kms_id, mock_kms_factory());
    kms_id
}

fn table_url(uri: &str) -> Url {
    Url::from_directory_path(uri).unwrap()
}

async fn create_encrypted_table(uri: &str, kms_id: &str) -> DeltaResult<()> {
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;
    table
        .create()
        .with_columns(get_table_columns())
        .with_table_name("test")
        .with_property("delta.encryption.kms_id", kms_id)
        .with_property("delta.encryption.footer_key", "test-footer-key")
        .await?;

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let batch = get_table_batches();
    let table = table.write(vec![batch.clone()]).await?;
    table.write(vec![batch]).await?;
    Ok(())
}

/// Assert every `.parquet` file under `dir` has an encrypted footer.
async fn assert_all_parquets_encrypted(dir: &std::path::Path) {
    use futures::TryStreamExt as _;
    use object_store::{ObjectMeta, ObjectStore as _, local::LocalFileSystem};
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use parquet::arrow::async_reader::ParquetObjectReader;

    let store = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let files: Vec<ObjectMeta> = store.list(None).try_collect().await.unwrap();
    let parquet: Vec<_> = files
        .into_iter()
        .filter(|meta| meta.location.extension() == Some("parquet"))
        .collect();
    assert!(!parquet.is_empty(), "no parquet files to verify encryption");
    for meta in parquet {
        let reader = ParquetObjectReader::new(store.clone(), meta.location.clone())
            .with_file_size(meta.size);
        assert!(
            ParquetRecordBatchStreamBuilder::new(reader).await.is_err(),
            "{} opened without decryption: not encrypted",
            meta.location
        );
    }
}

/// The parsed `stats` of every add action in version 1 of the table at `dir`.
fn add_stats(dir: &std::path::Path) -> Vec<serde_json::Value> {
    let log = std::fs::read_to_string(dir.join("_delta_log/00000000000000000001.json")).unwrap();
    let stats: Vec<serde_json::Value> = log
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter_map(|action| action.get("add").cloned())
        .map(|add| serde_json::from_str(add["stats"].as_str().expect("add has stats")).unwrap())
        .collect();
    assert!(!stats.is_empty(), "no add actions");
    stats
}

// ---------------------------------------------------------------------------
// Tests available at the enc-write-path stage
// (no DataFusion read-back required)
// ---------------------------------------------------------------------------

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
    create_encrypted_table(uri, &kms_id).await?;

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
    create_encrypted_table(uri, &kms_id).await?;
    let mut table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let version = table.version();

    let err = writer
        .flush()
        .await
        .expect_err("flush must refuse plaintext files for an encrypted table");
    assert!(err.to_string().contains("differs"), "{err}");
    let err = writer
        .flush_and_commit(&mut table)
        .await
        .expect_err("flush_and_commit must refuse plaintext files for an encrypted table");
    assert!(err.to_string().contains("differs"), "{err}");
    assert_eq!(table.version(), version);
    Ok(())
}

/// Replace `uri` with a table of the same schema and the given encryption properties.
async fn replace_table(uri: &str, properties: &[(&str, &str)]) -> DeltaResult<DeltaTable> {
    use deltalake_core::protocol::SaveMode;

    let mut create = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?
        .create()
        .with_columns(get_table_columns())
        .with_save_mode(SaveMode::Overwrite);
    for (key, value) in properties {
        create = create.with_property(*key, *value);
    }
    create.await
}

/// A writer's files are refused by a table whose frozen encryption settings differ from
/// the ones they were written under, not only by one that turned encryption on: here a
/// column-keyed writer against a table replaced with uniform encryption, and a plaintext
/// `JsonWriter` against a table replaced with encryption.
#[tokio::test]
async fn test_writers_refuse_tables_whose_encryption_settings_changed() -> DeltaResult<()> {
    use deltalake_core::writer::{DeltaWriter as _, JsonWriter, RecordBatchWriter};

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    let encryption = [
        ("delta.encryption.kms_id", kms_id.as_str()),
        ("delta.encryption.footer_key", "test-footer-key"),
    ];

    let column_keyed = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .build()?
        .create()
        .with_columns(get_table_columns())
        .with_property("delta.encryption.kms_id", kms_id.as_str())
        .with_property("delta.encryption.footer_key", "test-footer-key")
        .with_property("delta.encryption.column_keys", "test-key:int")
        .await?;
    let mut writer = RecordBatchWriter::for_table(&column_keyed)?;
    writer.write(get_table_batches()).await?;
    let mut uniform = replace_table(uri, &encryption).await?;
    let version = uniform.version();
    let err = writer
        .flush_and_commit(&mut uniform)
        .await
        .expect_err("column-keyed files must not enter a uniformly encrypted table");
    assert!(err.to_string().contains("column_keys"), "{err}");
    assert_eq!(uniform.version(), version);

    let plaintext = replace_table(uri, &[]).await?;
    let mut writer = JsonWriter::for_table(&plaintext)?;
    writer
        .write(vec![serde_json::json!({"int": 1, "string": "a"})])
        .await?;
    let mut encrypted = replace_table(uri, &encryption).await?;
    let version = encrypted.version();
    let err = writer
        .flush_and_commit(&mut encrypted)
        .await
        .expect_err("plaintext files must not enter an encrypted table");
    assert!(err.to_string().contains("differs"), "{err}");
    assert_eq!(encrypted.version(), version);
    Ok(())
}

/// A `try_new` writer that never wrote loads the table's configuration when it commits, so
/// an empty flush on an encrypted table is not mistaken for plaintext files.
#[tokio::test]
async fn test_record_batch_writer_try_new_commits_an_empty_window() -> DeltaResult<()> {
    use deltalake_core::writer::{DeltaWriter as _, RecordBatchWriter};

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    create_encrypted_table(uri, &kms_id).await?;
    let mut table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .load()
        .await?;
    let mut writer = RecordBatchWriter::try_new(
        table_url(uri).as_str(),
        get_table_batches().schema(),
        None,
        None,
    )?;
    writer.flush_and_commit(&mut table).await?;
    Ok(())
}

/// A column key may name a struct: every leaf under it is encrypted, so the write succeeds
/// and the log holds statistics for the plaintext column only.
#[tokio::test]
async fn test_column_key_on_a_struct_encrypts_its_leaves() -> DeltaResult<()> {
    use arrow::array::{ArrayRef, StructArray};
    use deltalake_core::kernel::StructType;

    let kms_id = register_fresh_factory();
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();
    let address = StructType::try_new([
        StructField::new("street", DataType::Primitive(PrimitiveType::String), true),
        StructField::new("city", DataType::Primitive(PrimitiveType::String), true),
    ])?;
    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?
        .build()?
        .create()
        .with_columns([
            StructField::new("id", DataType::Primitive(PrimitiveType::Integer), false),
            StructField::new("address", DataType::Struct(Box::new(address)), true),
        ])
        .with_property("delta.encryption.kms_id", kms_id.as_str())
        .with_property("delta.encryption.footer_key", "test-footer-key")
        .with_property("delta.encryption.column_keys", "pii:address")
        .await?;

    let street: ArrayRef = Arc::new(StringArray::from(vec!["1 Main St", "2 High St"]));
    let city: ArrayRef = Arc::new(StringArray::from(vec!["Springfield", "Shelbyville"]));
    let address = StructArray::from(vec![
        (
            Arc::new(Field::new("street", ArrowDataType::Utf8, true)),
            street,
        ),
        (
            Arc::new(Field::new("city", ArrowDataType::Utf8, true)),
            city,
        ),
    ]);
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef),
        ("address", Arc::new(address) as ArrayRef),
    ])
    .unwrap();
    table.write(vec![batch]).await?;

    for stats in add_stats(dir.path()) {
        assert_eq!(stats["numRecords"], 2, "{stats}");
        assert!(stats["minValues"].get("id").is_some(), "{stats}");
        assert!(stats["minValues"].get("address").is_none(), "{stats}");
        assert!(stats["nullCount"].get("address").is_none(), "{stats}");
    }
    assert_all_parquets_encrypted(dir.path()).await;
    Ok(())
}

/// The caller's parquet properties still apply to an encrypted table's files, and cannot
/// turn the encryption off: the encryption layer runs after them.
#[tokio::test]
async fn test_caller_properties_keep_encryption() -> DeltaResult<()> {
    use deltalake_core::datafile::DeltaWriterProperties;
    use deltalake_core::operations::write::encryption::apply_encryption_from_configuration;
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
    let zstd = Compression::ZSTD(ZstdLevel::try_new(3).unwrap());
    let props = apply_encryption_from_configuration(
        DeltaWriterProperties::default()
            .with_parquet_properties(WriterProperties::builder().set_compression(zstd).build()),
        &configuration,
        None,
    )?;
    let file_properties = props
        .resolve(
            &object_store::path::Path::from("part-0.parquet"),
            &get_table_batches().schema(),
        )
        .await?;
    assert_eq!(file_properties.compression(&ColumnPath::from("int")), zstd);
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

        for stats in add_stats(tmp.path()) {
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

    let stats = &add_stats(tmp.path())[0];
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
