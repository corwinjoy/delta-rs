use std::fs;
use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::collect_sendable_stream;
use deltalake::parquet::{
    file::properties::WriterProperties,
};
use deltalake::{parquet, DeltaOps};

use std::sync::Arc;
use futures::StreamExt;
use deltalake::datafusion::execution::runtime_env::RuntimeEnv;
use deltalake::datafusion::execution::{SessionState, SessionStateBuilder};
use deltalake::datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::parquet::encryption::decrypt::FileDecryptionProperties;
use deltalake::parquet::encryption::encrypt::FileEncryptionProperties;
use deltalake_core::datafusion::config::ConfigFileDecryptionProperties;
use deltalake_core::{DeltaTable, DeltaTableError};
use deltalake_core::logstore::LogStoreRef;
use url::Url;
use deltalake::arrow::datatypes::Schema;
use std::time::{Duration, Instant};
use deltalake::arrow::array::Float32Array;

fn get_table_columns() -> Vec<StructField> {
    vec![
        StructField::new(
            String::from("float"),
            DataType::Primitive(PrimitiveType::Float),
            false,
        ),
    ]
}

fn get_table_schema() -> Arc<Schema> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("float", ArrowDataType::Float32, false),
    ]));
    schema
}

fn get_table_batches() -> RecordBatch {
    let schema = get_table_schema();

    let float_values = Float32Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(float_values),
        ],
    )
    .unwrap()
}

async fn create_table(uri: &str, table_name: &str, crypt: &FileEncryptionProperties) -> Result<DeltaTable, DeltaTableError> {
    fs::remove_dir_all(uri)?;
    let ops = DeltaOps::try_from_uri(uri).await?;

    // The operations module uses a builder pattern that allows specifying several options
    // on how the command behaves. The builders implement `Into<Future>`, so once
    // options are set you can run the command using `.await`.
    let table = ops
        .create()
        .with_columns(get_table_columns())
        .with_table_name(table_name)
        .with_comment("A table to show how delta-rs works")
        .await?;

    assert_eq!(table.version(), 0);

    let writer_properties = WriterProperties::builder()
        .with_file_encryption_properties(crypt.clone())
        .build();

    let batch = get_table_batches();
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_writer_properties(writer_properties.clone())
        .await?;

    assert_eq!(table.version(), 1);

    Ok(table)
}

fn register_store(store: LogStoreRef, env: Arc<RuntimeEnv>) {
    let object_store_url = store.object_store_url();
    let url: &Url = object_store_url.as_ref();
    env.register_object_store(url, store.object_store(None));
}

async fn open_table_with_state(uri: &str, decryption_properties: &FileDecryptionProperties) -> Result<(DeltaTable, SessionState), DeltaTableError> {
    let table = deltalake::open_table(String::from(uri)).await?;
    let fd: ConfigFileDecryptionProperties = decryption_properties.clone().into();
    let mut sc = SessionConfig::new();
    sc.options_mut().execution.parquet.file_decryption_properties = Some(fd);

    let state = SessionStateBuilder::new()
        .with_config(sc)
        .build();

    register_store(table.log_store(), state.runtime_env().clone());
    Ok((table, state))
}

async fn read_table(uri: &str, decryption_properties: &FileDecryptionProperties) -> Result<(), deltalake::errors::DeltaTableError>{
    let (table, state) = open_table_with_state(uri, decryption_properties).await?;

    let (_table, mut stream) = DeltaOps(table).load()
        .with_session_state(state)
        .with_columns(vec!["float"]) // only read specified columns
        .await?;
    
    /*
    // Read and print full data
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;
    println!("{data:?}");
    
    */

    // Read data, streaming and discarding record batches:
    let mut batch_count = 0;
    while let Some(item) = stream.next().await {
        batch_count = batch_count + 1;
    }

    println!("batch_count {}", batch_count);
    Ok(())
}


async fn round_trip_test() -> Result<(), deltalake::errors::DeltaTableError> {
    let uri = "/home/cjoy/src/delta-rs-with-encryption/delta-rs/crates/deltalake/examples/encrypted_roundtrip";
    let table_name = "roundtrip";
    let key: Vec<_> = b"1234567890123450".to_vec();

    let crypt = parquet::encryption::encrypt::
        FileEncryptionProperties::builder(key.clone())
            .with_column_key("float", key.clone())
            .build()?;

    let decrypt = FileDecryptionProperties::builder(key.clone())
        .with_column_key("float", key.clone())
        .build()?;

    let start = Instant::now();
    create_table(uri, table_name, &crypt).await?;
    let duration = start.elapsed();
    println!("Time elapsed in create_table() is: {:?}", duration);

    let start = Instant::now();
    read_table(uri, &decrypt).await?;
    let duration = start.elapsed();
    println!("Time elapsed in read_table() is: {:?}", duration);
    Ok(())
}


#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    round_trip_test().await?;
    Ok(())
}

