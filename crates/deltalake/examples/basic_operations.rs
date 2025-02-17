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
use deltalake::{parquet, protocol::SaveMode, DeltaOps};

use std::sync::Arc;
use deltalake::datafusion::prelude::SessionConfig;
use deltalake_core::datafusion::config::ConfigFileDecryptionProperties;
use deltalake_core::{DeltaTable, DeltaTableError};

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

async fn create_table(uri: &str, table_name: &str, key: &Vec<u8>) -> Result<DeltaTable, DeltaTableError> {
    fs::remove_dir_all(uri)?;
    let ops = DeltaOps::try_from_uri(uri).await?;


    // The operations module uses a builder pattern that allows specifying several options
    // on how the command behaves. The builders implement `Into<Future>`, so once
    // options are set you can run the command using `.await`.
    let table = ops
        .create()
        .with_columns(get_table_columns())
        // .with_partition_columns(["timestamp"])
        .with_table_name(table_name)
        .with_comment("A table to show how delta-rs works")
        .await?;

    assert_eq!(table.version(), 0);


    let crypt = parquet::encryption::encryption::
    FileEncryptionProperties::builder(key.clone()).build().unwrap();

    let writer_properties = WriterProperties::builder()
        // .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_file_encryption_properties(crypt)
        .build();

    let batch = get_table_batches();
    let table = DeltaOps(table)
        .write(vec![batch.clone()])
        .with_writer_properties(writer_properties)
        .await?;

    assert_eq!(table.version(), 1);

    /*
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

     */
    Ok(table)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // Create a delta operations client pointing at an un-initialized location.
    /*
    let ops = if let Ok(table_uri) = std::env::var("TABLE_URI") {
        DeltaOps::try_from_uri(table_uri).await?
    } else {
        DeltaOps::new_in_memory()
    };
     */

    // let uri = "/home/cjoy/src/dl_benchmark/delta-rs/crates/deltalake/examples/tmp_tbl";
    // let table_name = "my_table";

    let uri = "/home/cjoy/src/delta-rs-with-encryption/delta-rs/crates/deltalake/examples/encrypted";
    let table_name = "deltars_table";
    // let key: Vec<_> = b"password".to_vec();

    const FOOTER_KEY: &[u8] = b"0123456789112345";
    const FOOTER_KEY_NAME: &str = "footer_key";
    const COL_KEY: &[u8] = b"1234567890123450";
    const COL_KEY_NAME: &str = "col_key";

    let key: Vec<_> = FOOTER_KEY.to_vec();

    /*
    def create_encryption_config(df):
        return pe.EncryptionConfiguration(
            footer_key=FOOTER_KEY_NAME,
            column_keys={
                COL_KEY_NAME: df.columns.tolist(),
            })

    column_keys = {'col_key': ['x0', 'x1', 'x2', 'x3', 'x4', 'x5', 'x6', 'x7', 'x8', 'x9', 'x10',
                               'x11', 'x12', 'x13', 'x14', 'x15', 'x16', 'x17', 'x18', 'x19']}

    def create_kms_connection_config():
        return pe.KmsConnectionConfig(
            custom_kms_conf={
                FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
                COL_KEY_NAME: COL_KEY.decode("UTF-8"),
            }
        )


     */

    // create_table(uri, table_name, &key).await?;

    let uri_table = String::from(uri) + "/" + table_name;

    let table = deltalake::open_table(uri_table).await?;
    let fd = ConfigFileDecryptionProperties {footer_key: String::from_utf8(key).unwrap(),
        ..Default::default()};
    let mut sc = SessionConfig::new();
    sc.options_mut().execution.parquet.file_decryption_properties = Some(fd);
    let (_table, stream) = DeltaOps(table).load().with_session_config(sc).await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{:?}", data);

    Ok(())
}


