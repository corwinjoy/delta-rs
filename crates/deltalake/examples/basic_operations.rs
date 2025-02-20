use std::collections::HashMap;
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
use deltalake::parquet::encryption::decryption::FileDecryptionProperties;
use deltalake_core::datafusion::config::{ConfigFileDecryptionProperties, EncryptionColumnKeys};
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

    // const FOOTER_KEY: &[u8] = b"0123456789112345";
    const FOOTER_KEY: &[u8] = b"\xd2\xcb \x89;\xb6\xab\xe4v\xa5\x1bB\xb8C\xbd\xed";
    const FOOTER_KEY_NAME: &str = "footer_key";
    const COL_KEY: &[u8] = b"1234567890123450";
    const COL_KEY_NAME: &str = "col_key";

    let key: Vec<_> = FOOTER_KEY.to_vec();

    let decryption_properties = FileDecryptionProperties::builder(b"\xd2\xcb \x89;\xb6\xab\xe4v\xa5\x1bB\xb8C\xbd\xed".to_vec())
        .with_column_key(b"x0".to_vec(), b"\xf9\x1b1\xac#\xcbT\xaa\xa6XWk:\xce\x04B".to_vec())
        .with_column_key(b"x1".to_vec(), b"(\xb0\x0e\x80F\xe1\xd6\xc5+\x19\x9c\xd7\xcbJ\x8ez".to_vec())
        .with_column_key(b"x2".to_vec(), b"\xe1#\xbe\x8f\x85\xc2,\xef>lD\xeb\x917\xa3\x15".to_vec())
        .with_column_key(b"x3".to_vec(), b"\xa4\xb6S\xa91pI\x16\xb1\x8cB\xdd.\xa9\\\\".to_vec())
        .with_column_key(b"x4".to_vec(), b"\x9d9\xa2\x16\xc1@5\x00\x0b\x94\xbf\xf6}\x1e\xe6.".to_vec())
        .with_column_key(b"x5".to_vec(), b"\xcbw\x03\xa0\\\xaa\x12K\xc7\x8b\x93E\x03jrM".to_vec())
        .with_column_key(b"x6".to_vec(), b"\xbb\x9d\xdcB<am\x8cU\xe4\x13\x94\x9a,\xcd}".to_vec())
        .with_column_key(b"x7".to_vec(), b"\x1bcC\xd6\xed\xbf\xe6\\\xa5\xad\xca\xbb\t*\x8e\x07".to_vec())
        .with_column_key(b"x8".to_vec(), b"^\xa0C\xea\x861\x18\xedxqd*\xb18\xea\x13".to_vec())
        .with_column_key(b"x9".to_vec(), b"K\xbaZ\x89`g\x86\x80o\xfe\x00\x1a7\xd4\xfbP".to_vec())
        .with_column_key(b"x10".to_vec(), b"\xc6\xab'.\xe7\xfb+\xe1\x16\xae\x07\x07\xd8\x13\xf2\xef".to_vec())
        .with_column_key(b"x11".to_vec(), b"S\x0f3\xf7l\xf6\xa6\x95,\xb5\x8d\r\x0b\xd9\xedc".to_vec())
        .with_column_key(b"x12".to_vec(), b"\x85\xe4\xaf\xa4\xb9E\xbc\xa3O\xcb\x02+)\x08\x95y".to_vec())
        .with_column_key(b"x13".to_vec(), b"\x99\x7fg\x89\x11|\x105B\x11y!7\xf5\x85\x04".to_vec())
        .with_column_key(b"x14".to_vec(), b" R\x0c.\xa6\xfdK\xe1\xa3\xd2&G%w\xa4\x91".to_vec())
        .with_column_key(b"x15".to_vec(), b"\x0e\xc8^\xa2\x893-r\x934\xa2\xa6\x194\xa6\xb5".to_vec())
        .with_column_key(b"x16".to_vec(), b"\xad\xa5&\x14\xad}o\x9bc\xe9\x8bdD:\xa8\xc4".to_vec())
        .with_column_key(b"x17".to_vec(), b"\xd6?\xfb\xba-s\xfe1\xef&z(\x84\x9eA\xb5".to_vec())
        .with_column_key(b"x18".to_vec(), b"\x00\x04\xdb\xc5Rj\xfb].\xff0\x83\xa3\xb2\x8fx".to_vec())
        .with_column_key(b"x19".to_vec(), b"\x1e\x80\x7f\x82\xf3\x15eb\xc5\x83\x81\xe21\xd6}b".to_vec())
        .build()
        .unwrap();


    let mut column_keys: HashMap<String, String> = HashMap::new();

    let mut count = 1;
    for (key, value) in decryption_properties.column_keys.unwrap().iter() {
        println!("{}", count);
        count = count + 1;
        column_keys.insert(hex::encode(key.clone()), hex::encode(value.clone()));
    }

    let eck = EncryptionColumnKeys{ column_keys_as_hex: column_keys };
    let json_col_keys = serde_json::to_string(&eck)?;
    // let json_col_keys = String::new();



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
    let fd = ConfigFileDecryptionProperties {footer_key_as_hex: hex::encode(key),
        column_keys_as_json_hex: json_col_keys,
        ..Default::default()};
    let mut sc = SessionConfig::new();
    sc.options_mut().execution.parquet.file_decryption_properties = Some(fd);
    let (_table, stream) = DeltaOps(table).load().with_session_config(sc).await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{:?}", data);

    Ok(())
}


