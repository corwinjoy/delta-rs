use std::{env, fs};
use deltalake::arrow::{
    datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use deltalake::kernel::{DataType, PrimitiveType, StructField};
use deltalake::operations::collect_sendable_stream;
use deltalake::parquet::{
    file::properties::WriterProperties,
};
use deltalake::{DeltaOps};

use std::sync::Arc;
use futures::StreamExt;
use deltalake::datafusion::execution::runtime_env::RuntimeEnv;
use deltalake::datafusion::execution::{SessionState, SessionStateBuilder};
use deltalake::datafusion::prelude::{SessionConfig};
use deltalake::parquet::encryption::decrypt::FileDecryptionProperties;
use deltalake::parquet::encryption::encrypt::FileEncryptionProperties;
use deltalake_core::datafusion::config::ConfigFileDecryptionProperties;
use deltalake_core::{DeltaTable, DeltaTableBuilder, DeltaTableError};
use deltalake_core::logstore::LogStoreRef;
use url::Url;
use deltalake::arrow::datatypes::Schema;
use std::time::{Duration, Instant};
use rand::{Rng};
use deltalake::arrow::array::{Array, Float32Array};
use deltalake_core::storage::DynObjectStore;
use deltalake_core::storage::object_store::local::LocalFileSystem;

mod crypt_fs;
use crypt_fs::{KMS, CryptFileSystem};

fn get_column_names(ncol: usize) -> Vec<String> {
    let mut colnames = Vec::new(); // Initialize an empty vector to hold the strings.

    for i in 0..ncol {
        let colname = format!("flt{}", i);
        colnames.push(colname);
    }
    colnames
}

fn get_table_columns(ncol: usize) -> Vec<StructField> {
    let mut cols = Vec::new(); // Initialize an empty vector to hold the strings.
    let colnames = get_column_names(ncol);

    for i in 0..ncol {
        let col = StructField::new(
            colnames[i].clone(),
            DataType::Primitive(PrimitiveType::Float),
            false,
        );
        cols.push(col); // Add the new String to the vector.
    }

    cols
}

fn get_table_schema(ncol: usize) -> Arc<Schema> {
    let mut fields = Vec::new();
    let colnames = get_column_names(ncol);
    for i in 0..ncol {
        let field = Field::new(
            colnames[i].clone(),
            ArrowDataType::Float32,
            false

        );
        fields.push(field); // Add the new String to the vector.
    }
    let schema = Arc::new(ArrowSchema::new(fields));
    schema
}

fn get_table_batches(ncol: usize, nrow: usize) -> RecordBatch {
    let mut rng = rand::rng();
    let schema = get_table_schema(ncol);

    let mut columns: Vec<Arc<dyn Array>> = Vec::new();
    for _i in 0..ncol {
        let mut vals: Vec<f32> = Vec::with_capacity(nrow);
        for _j in 0..nrow {
            vals.push(rng.random());
        }
        let float_values = Float32Array::from(vals);
        columns.push(Arc::new(float_values));
    }

    let batches = RecordBatch::try_new(
        schema,
        columns,
    ).unwrap();

    batches
}

async fn create_table(uri: &str, table_name: &str, crypt: Option<&FileEncryptionProperties>,
                      ncol: usize, nrow: usize, object_store: Arc<DynObjectStore>) -> Result<DeltaTable, DeltaTableError> {
    let mut table = DeltaTableBuilder::from_valid_uri(uri)
        .unwrap()
        .with_storage_backend(object_store, Url::parse(uri).unwrap())
        .build()?;

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
        .with_columns(get_table_columns(ncol))
        .with_table_name(table_name)
        .with_comment("A table to show how delta-rs works")
        .await?;

    assert_eq!(table.version(), 0);

    let mut writer_properties = WriterProperties::builder();

    if let Some(crypt) = crypt {
        writer_properties = writer_properties.with_file_encryption_properties(crypt.clone());
    }

    let writer_properties = writer_properties.build();

    let batch = get_table_batches(ncol, nrow);
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

async fn open_table_with_state(uri: &str, decryption_properties: Option<&FileDecryptionProperties>,
                               object_store: Arc<DynObjectStore>) -> Result<(DeltaTable, SessionState), DeltaTableError> {
    // let table = deltalake::open_table(String::from(uri)).await?;
    let table = DeltaTableBuilder::from_valid_uri(uri)?
        .with_storage_backend(object_store, Url::parse(uri).unwrap())
        .load().await?;

    let mut sc = SessionConfig::new();
    if let Some(decryption) = decryption_properties {
        let fd: ConfigFileDecryptionProperties = decryption.clone().into();
        sc.options_mut().execution.parquet.file_decryption_properties = Some(fd);
    }

    let state = SessionStateBuilder::new()
        .with_config(sc)
        .build();

    register_store(table.log_store(), state.runtime_env().clone());
    Ok((table, state))
}

async fn read_table(uri: &str, decryption_properties: Option<&FileDecryptionProperties>,
        columns: &Vec<String>, object_store: Arc<DynObjectStore>) -> Result<(), deltalake::errors::DeltaTableError>{
    let (table, state) = open_table_with_state(uri, decryption_properties, object_store).await?;

    let (_table, mut stream) = DeltaOps(table).load()
        .with_session_state(state)
        .with_columns(columns) // only read specified columns
        .await?;

    /*
    // Read and print full data
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;
    println!("{data:?}");

    */

    // Read data, streaming and discarding record batches:
    let mut batch_count = 0;
    while let Some(_item) = stream.next().await {
        batch_count = batch_count + 1;
    }

    // println!("batch_count {}", batch_count);
    Ok(())
}


async fn round_trip_test(crypt: Option<&FileEncryptionProperties>,
                         decrypt: Option<&FileDecryptionProperties>,
                         ncol:usize, nrow:usize,
                         colnames: &Vec<String>,
                         object_store: Arc<DynObjectStore>,
                         uri: &str) -> Result<(Duration, Duration), DeltaTableError> {

    let parsed_url: Url = Url::parse(uri.as_ref()).unwrap();
    let path = parsed_url.to_file_path().unwrap();
    let _ = fs::remove_dir_all(path.clone());
    fs::create_dir(path)?;

    let table_name = "roundtrip";

    let start = Instant::now();
    create_table(uri, table_name, crypt, ncol, nrow, object_store.clone()).await?;
    let duration_write = start.elapsed();
    // println!("Time elapsed in create_table() is: {:?}", duration);

    let start = Instant::now();
    read_table(uri, decrypt, &colnames, object_store.clone()).await?;
    let duration_read = start.elapsed();
    // println!("Time elapsed in read_table() is: {:?}", duration);
    Ok((duration_write, duration_read))
}

async fn run_roundtrip_test(ncol: usize, ncol_selected: usize, nrow: usize, all_colnames: &Vec<String>, use_modular_encryption: bool,
    object_store_name: &str, object_store: Arc<DynObjectStore>, uri: &str)
    -> Result<(Duration, Duration), DeltaTableError> {
    let key: Vec<_> = b"1234567890123450".to_vec();
    let selected_colnames = all_colnames[(ncol-ncol_selected)..ncol].to_vec();

    let mut crypt_builder = FileEncryptionProperties::builder(key.clone());
    for i in 0..ncol {
        crypt_builder = crypt_builder.with_column_key(&*all_colnames[i], key.clone());
    }
    let crypt = crypt_builder.build()?;

    let mut decrypt_builder = FileDecryptionProperties::builder(key.clone());
    for i in 0..ncol {
        decrypt_builder = decrypt_builder.with_column_key(&*all_colnames[i], key.clone());
    }
    let decrypt = decrypt_builder.build()?;

    let (opt_crypt, opt_decrypt) = match use_modular_encryption {
        true => { (Some(&crypt), Some(&decrypt)) },
        false => { (None, None) },
    };

    // println!("***************************************************************");
    // println!("Round trip test with Encryption = {}", use_modular_encryption);
    let (duration_write, duration_read) =
        round_trip_test(opt_crypt, opt_decrypt, ncol, nrow, &selected_colnames, object_store, uri).await?;

    if object_store_name == "Warmup" {
        println!("nrow, ncol, ncol_selected, use_modular_encryption, object_store_name, duration_write (s), duration_read (s)");
    } else {
        println!("{}, {}, {}, {}, {}, {}, {}", nrow, ncol, ncol_selected, use_modular_encryption, object_store_name,
                 duration_write.as_secs_f32(), duration_read.as_secs_f32());
    }

    Ok((duration_write, duration_read))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), DeltaTableError> {
    let ncol: usize = 10;
    let nrow: usize = 20;
    let dir = "/benchmark_crypt";
    let workdir = env::current_dir()?;
    let path = String::from(workdir.to_str().unwrap()) + dir;
    let path = path.as_str();
    let joined = String::from("file://") + path;
    let uri = joined.as_str();

    let _ = fs::remove_dir_all(path);
    fs::create_dir(path)?;

    let lfs_store = Arc::new(LocalFileSystem::new_with_prefix(path)?);
    let kms = Arc::new(KMS::new(b"password"));
    let encrypted_store = Arc::new(CryptFileSystem::new(uri, kms.clone())?);

    // warmup
    let colnames = get_column_names(ncol);
    run_roundtrip_test(ncol, ncol, nrow, &colnames, false, "Warmup", lfs_store.clone(), uri).await?;

    let ncols = vec![1_000, 5_000, 10_000];
    let nrows = vec![1_000, 10_000, 100_0000];

    // let ncols = vec![100, 500];
    // let nrows = vec![1000, 5000];
    for ncol in ncols.iter() {
        for nrow in nrows.iter() {
            benchmark_nrow_ncol(*ncol, *nrow, uri, lfs_store.clone(), encrypted_store.clone()).await?;
        }
    }
    

    Ok(())
}

async fn benchmark_nrow_ncol(ncol: usize, nrow: usize, uri: &str, lfs_store: Arc<LocalFileSystem>, encrypted_store: Arc<CryptFileSystem>) -> Result<(), DeltaTableError> {
    let colnames = get_column_names(ncol);

    // no parquet encryption, LFS
    run_roundtrip_test(ncol, ncol, nrow, &colnames, false, "LocalFileSystem", lfs_store.clone(), uri).await?;
    run_roundtrip_test(ncol, ncol / 10, nrow, &colnames, false, "LocalFileSystem", lfs_store.clone(), uri).await?;

    // modular encryption, LFS
    run_roundtrip_test(ncol, ncol, nrow, &colnames, true, "LocalFileSystem", lfs_store.clone(), uri).await?;
    run_roundtrip_test(ncol, ncol / 10, nrow, &colnames, true, "LocalFileSystem", lfs_store.clone(), uri).await?;

    // no parquet encryption, encrypted_file_store
    run_roundtrip_test(ncol, ncol, nrow, &colnames, false, "CryptFileSystem", encrypted_store.clone(), uri).await?;
    run_roundtrip_test(ncol, ncol / 10, nrow, &colnames, false, "CryptFileSystem", encrypted_store.clone(), uri).await?;
    Ok(())
}