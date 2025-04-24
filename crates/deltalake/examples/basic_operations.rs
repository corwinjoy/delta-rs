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
use deltalake::{protocol::SaveMode, DeltaOps};

use std::sync::Arc;
use deltalake_core::{DeltaTableBuilder, DeltaTableError};
use url::Url;
use deltalake_core::logstore::object_store::local::LocalFileSystem;
mod crypt_fs;

use crypt_fs::CryptFileSystem;

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

    let int_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let str_values = StringArray::from(vec!["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]);
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

/*
    async fn test_peek_with_invalid_json() -> DeltaResult<()> {
        use crate::logstore::object_store::memory::InMemory;
        let memory_store = Arc::new(InMemory::new());
        let log_path = Path::from("delta-table/_delta_log/00000000000000000001.json");

        let log_content = r#"{invalid_json"#;

        memory_store
            .put(&log_path, log_content.into())
            .await
            .expect("Failed to write log file");

        let table_uri = "memory:///delta-table";

        let table = crate::DeltaTableBuilder::from_valid_uri(table_uri)
            .unwrap()
            .with_storage_backend(memory_store, Url::parse(table_uri).unwrap())
            .build()?;

        let result = table.log_store().peek_next_commit(0).await;
        assert!(result.is_err());
        Ok(())
    }
 */

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // Create a delta operations client pointing at an un-initialized location.
    let path = "/home/cjoy/src/delta-rs/crates/deltalake/examples/test_crypt";
    let joined = String::from("file://") + path;
    let table_uri = joined.as_str();
    let file_store = Arc::new(CryptFileSystem::new()); // Starting ObjectStore

    let _ = fs::remove_dir_all(path);
    fs::create_dir(path)?;
    
    let mut table = DeltaTableBuilder::from_valid_uri(table_uri)
        .unwrap()
        .with_storage_backend(file_store, Url::parse(table_uri).unwrap())
        .build()?;
    
    // We allow for uninitialized locations, since we may want to create the table
    let ops: DeltaOps = match table.load().await {
        Ok(_) => Ok(table.into()),
        Err(DeltaTableError::NotATable(_)) => Ok(table.into()),
        Err(err) => Err(err),
    }?;

    // The operations module uses a builder pattern that allows specifying several options
    // on how the command behaves. The builders implement `Into<Future>`, so once
    // options are set you can run the command using `.await`.
    let table = ops
        .create()
        .with_columns(get_table_columns())
        .with_partition_columns(["timestamp"])
        .with_table_name("my_table")
        .with_comment("A table to show how delta-rs works")
        .await?;

    assert_eq!(table.version(), 0);

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let batch = get_table_batches();
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_writer_properties(writer_properties)
        .await?;

    assert_eq!(table.version(), 1);

    let writer_properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    // To overwrite instead of append (which is the default), use `.with_save_mode`:
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_save_mode(SaveMode::Overwrite)
        .with_writer_properties(writer_properties)
        .await?;

    assert_eq!(table.version(), 2);

    let (_table, stream) = DeltaOps(table).load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{data:?}");

    Ok(())
}
