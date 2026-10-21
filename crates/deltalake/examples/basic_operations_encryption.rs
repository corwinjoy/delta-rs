//! End-to-end example: Parquet encryption via Delta table properties.
//!
//! Encryption is configured by setting `delta.encryption.*` table properties at table
//! creation time.  A factory is registered once globally (or per-session); writes, DataFusion
//! scans and the table operations built on them (optimize, update, delete, merge, the change
//! feed) then encrypt and decrypt automatically. The raw `ParquetTableReader` has no keys
//! and refuses encrypted tables.
//!
//! Run with:
//! ```shell
//! cargo run --example basic_operations_encryption --features "datafusion encryption" -p deltalake
//! ```

use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::datafusion::{
    assert_batches_sorted_eq,
    prelude::{SessionContext, col, lit},
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::optimize::OptimizeType;
use deltalake::{DeltaTable, DeltaTableError};
use deltalake_core::operations::write::encryption::parquet_key_management::{
    crypto_factory::CryptoFactory,
    errors::{Error as KmsError, Result as KmsResult},
    kms::{KmsClient, KmsClientRef, KmsConnectionInfo},
};
use deltalake_core::operations::write::encryption::{
    KmsEncryptionFactory, register_encryption_factory,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

const KMS_ID: &str = "my-test-kms";

/// A stand-in for a key management service.
///
/// delta-rs's reference `KmsEncryptionFactory` is built on the `parquet-key-management`
/// crate, which generates fresh data keys for every file and asks the KMS to wrap them under
/// the master key IDs named in the table properties; the wrapped keys are stored in the
/// file's own Parquet key metadata, and readers ask the KMS to unwrap them again. A real
/// [`KmsClient`] encrypts the key with the master key held by the service. This one just
/// keeps the keys in memory under a counter token, so it only works within one process,
/// which is all an example needs.
#[derive(Debug, Default)]
struct InMemoryKms {
    vault: Mutex<HashMap<String, Vec<u8>>>,
    next_token: AtomicU64,
}

impl KmsClient for InMemoryKms {
    fn wrap_key(&self, key: &[u8], master_key_id: &str) -> KmsResult<String> {
        println!("KMS: wrapping a data key under master key '{master_key_id}'");
        let token = self.next_token.fetch_add(1, Ordering::Relaxed).to_string();
        self.vault
            .lock()
            .unwrap()
            .insert(token.clone(), key.to_vec());
        Ok(token)
    }

    fn unwrap_key(&self, wrapped_key: &str, master_key_id: &str) -> KmsResult<Vec<u8>> {
        self.vault
            .lock()
            .unwrap()
            .get(wrapped_key)
            .cloned()
            .ok_or_else(|| {
                KmsError::General(format!(
                    "unknown wrapped key for master key '{master_key_id}'"
                ))
            })
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
    url::Url::from_directory_path(uri).unwrap()
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
    // The crate asks the client factory for a client per KMS connection (instance ID,
    // URL, access token) and caches it; `KmsConnectionInfo` carries the table's
    // `delta.encryption.kms_configuration` as a custom option for clients that need it.
    // delta-rs builds one `CryptoFactory` per distinct `kms_configuration`.
    let kms: KmsClientRef = Arc::new(InMemoryKms::default());
    let factory = KmsEncryptionFactory::new(move || {
        let kms = Arc::clone(&kms);
        CryptoFactory::new(move |_: &KmsConnectionInfo| Ok(Arc::clone(&kms)))
    });
    register_encryption_factory(KMS_ID, Arc::new(factory));
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
        .with_configuration([
            ("delta.encryption.kms_id", Some(KMS_ID)),
            ("delta.encryption.footer_key", Some("my-footer-master-key")),
        ])
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
