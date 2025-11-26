use deltalake::arrow::array::RecordBatch;
use deltalake_core::operations::collect_sendable_stream;
use deltalake_core::DeltaOps;
use url::Url;

use deltalake_core::datafusion::assert_batches_sorted_eq;
use deltalake_core::datafusion::common::test_util::format_batches;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), deltalake::errors::DeltaTableError> {
    // TODO: use a relative path here.
    let table_path = "/home/cjoy/src/delta-rs/crates/test/tests/data/delta-0.8.0-full-path";
    let table_url = Url::from_directory_path(table_path).unwrap();
    let table = deltalake::open_table(table_url).await?;
    println!("{table}");
    let files: Vec<String> = table.get_file_uris()?.collect();
    // Collect iterator into a Vec so it implements Debug for pretty printing
    println!("{:?}", files);

    let mut expected: Vec<String> = vec![
        "/home/cjoy/src/delta-rs/crates/test/tests/data/delta-0.8.0-full-path/part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet".to_string(),
        "/home/cjoy/src/delta-rs/crates/test/tests/data/delta-0.8.0-full-path/part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet".to_string(),
    ];

    // Compare ignoring order
    let mut files_sorted = files;
    files_sorted.sort();
    expected.sort();
    let expected_sorted = expected;
    assert_eq!(files_sorted, expected_sorted);

    let (_table, stream) = DeltaOps(table).load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{data:?}");

    // TODO: use a relative path here.
    let expected_table_path = "/home/cjoy/src/delta-rs/crates/test/tests/data/delta-0.8.0";
    let expected_table_url = Url::from_directory_path(expected_table_path).unwrap();
    let expected_table = deltalake::open_table(expected_table_url).await?;
    let (_table, stream) = DeltaOps(expected_table).load().await?;
    let expected_data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{expected_data:?}");

    let expected_lines = format_batches(&*expected_data)?.to_string();
    let expected_lines_vec: Vec<&str> = expected_lines.trim().lines().collect();
    assert_batches_sorted_eq!(&expected_lines_vec, &data);
    Ok(())
}
