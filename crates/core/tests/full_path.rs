// Verifies that tables with fully-qualified file paths behave as expected
// and that reading them yields the same data as the corresponding table
// with relative file paths.

use std::fs;
use std::path::{Path, PathBuf};
use url::Url;

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn read_table_with_full_paths_and_compare() {
    use deltalake_core::arrow::array::RecordBatch;
    use deltalake_core::datafusion::assert_batches_sorted_eq;
    use deltalake_core::datafusion::common::test_util::format_batches;
    use deltalake_core::operations::collect_sendable_stream;
    use deltalake_core::DeltaOps;

    // Use relative paths and resolve them to absolute by canonicalizing.
    let full_path_rel = Path::new("../test/tests/data/delta-0.8.0-full-path");
    let full_path_abs: PathBuf = fs::canonicalize(full_path_rel).unwrap();
    let table = deltalake_core::open_table(Url::from_directory_path(&full_path_abs).unwrap())
        .await
        .unwrap();

    // The table contains fully-qualified add file paths; ensure we get the
    // expected absolute file paths back from the table API.
    let mut files: Vec<String> = table.get_file_uris().unwrap().collect();

    let mut expected: Vec<String> = vec![
        full_path_abs
            .join(
                "part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet",
            )
            .to_str()
            .unwrap()
            .to_string(),
        full_path_abs
            .join(
                "part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet",
            )
            .to_str()
            .unwrap()
            .to_string(),
    ];

    files.sort();
    expected.sort();
    assert_eq!(files, expected);

    // Load data from the full-path table.
    let (_table, stream) = DeltaOps(table).load().await.unwrap();
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await.unwrap();

    // Load data from the equivalent table with relative paths and compare.
    let expected_rel = Path::new("../test/tests/data/delta-0.8.0");
    let expected_abs = fs::canonicalize(expected_rel).unwrap();
    let expected_table =
        deltalake_core::open_table(Url::from_directory_path(&expected_abs).unwrap())
            .await
            .unwrap();
    let (_table, stream) = DeltaOps(expected_table).load().await.unwrap();
    let expected_data: Vec<RecordBatch> =
        collect_sendable_stream(stream).await.unwrap();

    let expected_lines = format_batches(&*expected_data).unwrap().to_string();
    let expected_lines_vec: Vec<&str> = expected_lines.trim().lines().collect();
    assert_batches_sorted_eq!(&expected_lines_vec, &data);
}
