use std::fs;
use deltalake::arrow::{
    array::{Int32Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, TimeUnit},
    record_batch::RecordBatch,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::collect_sendable_stream;
use deltalake::parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use deltalake::{arrow, parquet, protocol::SaveMode, DeltaOps};

use std::sync::Arc;
use deltalake::datafusion::execution::runtime_env::RuntimeEnv;
use deltalake::datafusion::execution::{SessionState, SessionStateBuilder};
use deltalake::datafusion::prelude::{SessionConfig, SessionContext};
use deltalake::parquet::encryption::decrypt::FileDecryptionProperties;
use deltalake::parquet::encryption::encrypt::FileEncryptionProperties;
use deltalake_core::datafusion::config::ConfigFileDecryptionProperties;
use deltalake_core::{DeltaTable, DeltaTableError, TableProperty};
use deltalake_core::logstore::LogStoreRef;
use url::Url;
use deltalake::arrow::datatypes::Schema;
use deltalake::datafusion::assert_batches_sorted_eq;

use deltalake::datafusion::dataframe::DataFrame;
use deltalake::datafusion::logical_expr::{col, lit};


fn get_table_columns() -> Vec<StructField> {
    vec![
        StructField::new(
            String::from("int"),
            DataType::Primitive(PrimitiveType::Integer),
            false,
        ),
        StructField::new(
            String::from("string"),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            String::from("timestamp"),
            DataType::Primitive(PrimitiveType::TimestampNtz),
            true,
        ),
    ]
}

fn get_table_schema() -> Arc<Schema> {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("int", ArrowDataType::Int32, false),
        Field::new("string", ArrowDataType::Utf8, true),
        Field::new(
            "timestamp",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]));
    schema
}

fn get_table_batches() -> RecordBatch {
    let schema = get_table_schema();

    let int_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let str_values = StringArray::from(vec!["A", "B", "C", "B", "A", "C", "A", "B", "B", "A", "A"]);
    let ts_values = TimestampMicrosecondArray::from(vec![
        1000000012, 1000000012, 1000000012, 1000000012, 500012305, 500012305, 500012305, 500012305,
        500012305, 500012305, 500012305,
    ]);
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(int_values),
            Arc::new(str_values),
            Arc::new(ts_values),
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

    // To overwrite instead of append (which is the default), use `.with_save_mode`:
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Overwrite)
        .with_writer_properties(writer_properties.clone())
        .await?;

    assert_eq!(table.version(), 2);

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

    let (_table, stream) = DeltaOps(table).load()
        .with_session_state(state)
        .await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{data:?}");

    Ok(())
}

async fn update_table(uri: &str, decryption_properties: &FileDecryptionProperties, crypt: &FileEncryptionProperties) -> Result<(), DeltaTableError> {
    let (table, state) = open_table_with_state(uri, decryption_properties).await?;
    let writer_properties = WriterProperties::builder()
        .with_file_encryption_properties(crypt.clone())
        .build();

    let (table, _metrics) = DeltaOps(table)
        .update()
        .with_session_state(state)
        .with_writer_properties(writer_properties)
        .with_predicate(col("int").eq(lit(1)))
        .with_update("int", "100")
        .await
        .unwrap();

    assert_eq!(table.version(), 3);

    Ok(())
}

async fn delete_from_table(uri: &str, decryption_properties: &FileDecryptionProperties, crypt: &FileEncryptionProperties) -> Result<(), DeltaTableError> {
    let (table, state) = open_table_with_state(uri, decryption_properties).await?;
    let writer_properties = WriterProperties::builder()
        .with_file_encryption_properties(crypt.clone())
        .build();

    let (table, _metrics) = DeltaOps(table)
        .delete()
        .with_session_state(state)
        .with_writer_properties(writer_properties)
        .with_predicate(col("int").eq(lit(2)))
        .await
        .unwrap();

    assert_eq!(table.version(), 3);

    Ok(())
}

fn merge_source(schema: Arc<ArrowSchema>) -> DataFrame {
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::Int32Array::from(vec![10, 20, 30])),
            Arc::new(arrow::array::StringArray::from(vec!["B", "C", "X"])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1000000012, 1000000012, 1000000012
            ])),
        ],
    ).unwrap();
    ctx.read_batch(batch).unwrap()
}


async fn merge_table(uri: &str, decryption_properties: &FileDecryptionProperties, crypt: &FileEncryptionProperties) -> Result<(), DeltaTableError> {

    let (table, state) = open_table_with_state(uri, decryption_properties).await?;
    let writer_properties = WriterProperties::builder()
        .with_file_encryption_properties(crypt.clone())
        .build();

    let schema = get_table_schema();
    let source = merge_source(schema);

    let (table, _metrics) = DeltaOps(table)
        .merge(source, col("target.int").eq(col("source.int")))
        .with_session_state(state.clone())
        .with_writer_properties(writer_properties)
        .with_source_alias("source")
        .with_target_alias("target")
        .when_not_matched_by_source_delete(|delete| {
            delete
        })
        .unwrap()
        .await
        .unwrap();

    let expected = vec![
        "+----+-------+------------+",
        "| id | value | modified   |",
        "+----+-------+------------+",
        "| A  | 1     | 2021-02-01 |",
        "| B  | 10    | 2021-02-01 |",
        "| C  | 10    | 2021-02-02 |",
        "+----+-------+------------+",
    ];

    let (_table, stream) = DeltaOps(table).load()
        .with_session_state(state)
        .await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{data:?}");

    assert_batches_sorted_eq!(&expected, &data);
    Ok(())
}

async fn round_trip_test() -> Result<(), deltalake::errors::DeltaTableError> {
    let uri = "/home/cjoy/src/delta-rs-with-encryption/delta-rs/crates/deltalake/examples/encrypted_roundtrip";
    let table_name = "roundtrip";
    let key: Vec<_> = b"1234567890123450".to_vec();
    let wrong_key: Vec<_> = b"9234567890123450".to_vec();

    let crypt = parquet::encryption::encrypt::
        FileEncryptionProperties::builder(key.clone())
            .with_column_key("int", key.clone())
            .with_column_key("string", key.clone())
            .build()?;

    let decrypt = FileDecryptionProperties::builder(key.clone())
        .with_column_key("int", key.clone())
        .with_column_key("string", key.clone())
        .build()?;

    //create_table(uri, table_name, &crypt).await?;
    update_table(uri, &decrypt, &crypt).await?;
    // delete_from_table(uri, &decrypt, &crypt).await?;
    //merge_table(uri, &decrypt, &crypt).await?;
    read_table(uri, &decrypt).await?;
    Ok(())
}


#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    //encrypted_read_test().await?;
    round_trip_test().await?;
    Ok(())
}

