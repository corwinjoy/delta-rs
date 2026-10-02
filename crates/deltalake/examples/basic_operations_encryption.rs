//! End-to-end example: Parquet encryption via Delta table properties.
//!
//! Encryption is configured by setting `delta.encryption.*` table properties at table
//! creation time.  A factory is registered once globally (or per-session) and all
//! subsequent read and write operations on the table automatically apply encryption.
//!
//! Run with:
//! ```shell
//! cargo run --example basic_operations_encryption --features "datafusion encryption" -p deltalake
//! ```

use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::datafusion::{
    assert_batches_sorted_eq,
    config::EncryptionFactoryOptions,
    execution::parquet_encryption::EncryptionFactory,
    prelude::{SessionContext, col, lit},
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::optimize::OptimizeType;
use deltalake::parquet::encryption::{
    decrypt::FileDecryptionProperties, encrypt::FileEncryptionProperties,
};
use deltalake::{DeltaTable, DeltaTableError, Path};
use deltalake_core::operations::write::encryption::register_encryption_factory;
use std::sync::Arc;
use tempfile::TempDir;

const KMS_ID: &str = "my-test-kms";

/// A stand-in for a KMS client: encrypts every file with one fixed key.
///
/// A real [`EncryptionFactory`] would use the key IDs delta-rs forwards in `options`
/// (`footer_key`, `column_keys`, `plaintext_footer`; see `EncryptionConfig::factory_options`)
/// to fetch or derive keys from a key management service, and bind each file's keys to its
/// table-relative `file_path`.
#[derive(Debug)]
struct FixedKeyFactory {
    key: Vec<u8>,
}

#[async_trait::async_trait]
impl EncryptionFactory for FixedKeyFactory {
    async fn get_file_encryption_properties(
        &self,
        _options: &EncryptionFactoryOptions,
        _schema: &SchemaRef,
        _file_path: &Path,
    ) -> deltalake::datafusion::error::Result<Option<Arc<FileEncryptionProperties>>> {
        Ok(Some(
            FileEncryptionProperties::builder(self.key.clone()).build()?,
        ))
    }

    async fn get_file_decryption_properties(
        &self,
        _options: &EncryptionFactoryOptions,
        _file_path: &Path,
    ) -> deltalake::datafusion::error::Result<Option<Arc<FileDecryptionProperties>>> {
        Ok(Some(
            FileDecryptionProperties::builder(self.key.clone()).build()?,
        ))
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("int", ArrowDataType::Int32, false),
        Field::new("string", ArrowDataType::Utf8, true),
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]))
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 10, 10])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "A"])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                500012305, 500012305, 500012305, 500012305,
            ])),
        ],
    )
    .unwrap()
}

fn table_url(uri: &str) -> url::Url {
    url::Url::parse(&format!("file://{}", uri)).unwrap()
}

async fn table_from_uri(uri: &str) -> DeltaTable {
    deltalake_core::DeltaTableBuilder::from_url(table_url(uri))
        .unwrap()
        .load()
        .await
        .expect("Failed to load table")
}

async fn read(uri: &str) -> Vec<RecordBatch> {
    let table = table_from_uri(uri).await;
    let ctx = SessionContext::new();
    ctx.register_table("t", table.table_provider().await.unwrap())
        .unwrap();
    ctx.sql("SELECT * FROM t ORDER BY int")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), DeltaTableError> {
    // -----------------------------------------------------------------------
    // Step 1: Register the KMS factory (done once at application startup).
    // -----------------------------------------------------------------------
    let factory = Arc::new(FixedKeyFactory {
        key: b"0123456789abcdef".to_vec(),
    });
    register_encryption_factory(KMS_ID, factory);
    println!("Registered KMS factory '{KMS_ID}'");

    // -----------------------------------------------------------------------
    // Step 2: Create an encrypted table using delta.encryption.* properties.
    // -----------------------------------------------------------------------
    let dir = TempDir::new()?;
    let uri = dir.path().to_str().unwrap();

    let table = deltalake_core::DeltaTableBuilder::from_url(table_url(uri))?.build()?;

    table
        .create()
        .with_columns(vec![
            StructField::new("int", DataType::Primitive(PrimitiveType::Integer), false),
            StructField::new("string", DataType::Primitive(PrimitiveType::String), true),
            StructField::new(
                "timestamp",
                DataType::Primitive(PrimitiveType::TimestampNtz),
                true,
            ),
        ])
        .with_table_name("encrypted_table")
        // These properties are stored in the delta log and automatically applied to all
        // subsequent operations — no per-operation encryption config needed.
        .with_property("delta.encryption.kms_id", KMS_ID)
        .with_property("delta.encryption.footer_key", "my-footer-master-key")
        .await?;

    println!("Created encrypted table at {uri}");

    // -----------------------------------------------------------------------
    // Step 3: Write data — automatically encrypted.
    // -----------------------------------------------------------------------
    let table = table_from_uri(uri).await;
    let table = table.write(vec![batch()]).await?;
    let _table = table.write(vec![batch()]).await?;
    println!("Wrote 2 batches (encrypted)");

    // -----------------------------------------------------------------------
    // Step 4: Optimize (Z-order + compact) — reads encrypted, writes encrypted.
    // -----------------------------------------------------------------------
    let table = table_from_uri(uri).await;
    let (_table, metrics) = table
        .optimize()
        .with_type(OptimizeType::ZOrder(vec!["int".to_string()]))
        .await?;
    println!("Z-order: {metrics:?}");

    let table = table_from_uri(uri).await;
    let (_table, metrics) = table.optimize().await?;
    println!("Compact: {metrics:?}");

    // -----------------------------------------------------------------------
    // Step 5: Delete and update — rewritten files are encrypted too.
    // -----------------------------------------------------------------------
    let table = table_from_uri(uri).await;
    let (table, metrics) = table.delete().with_predicate(col("int").eq(lit(2))).await?;
    println!(
        "Deleted {} rows",
        metrics.num_deleted_rows.unwrap_or_default()
    );
    let (_table, metrics) = table
        .update()
        .with_predicate(col("int").eq(lit(10)))
        .with_update("string", lit("C"))
        .await?;
    println!("Updated {} rows", metrics.num_updated_rows);

    // -----------------------------------------------------------------------
    // Step 6: Read back — automatically decrypted.
    // -----------------------------------------------------------------------
    let batches = read(uri).await;
    println!("Final table:");
    assert_batches_sorted_eq!(
        &[
            "+-----+--------+----------------------------+",
            "| int | string | timestamp                  |",
            "+-----+--------+----------------------------+",
            "| 1   | A      | 1970-01-01T00:08:20.012305 |",
            "| 1   | A      | 1970-01-01T00:08:20.012305 |",
            "| 10  | C      | 1970-01-01T00:08:20.012305 |",
            "| 10  | C      | 1970-01-01T00:08:20.012305 |",
            "| 10  | C      | 1970-01-01T00:08:20.012305 |",
            "| 10  | C      | 1970-01-01T00:08:20.012305 |",
            "+-----+--------+----------------------------+",
        ],
        &batches
    );
    println!("All data verified successfully.");
    Ok(())
}
