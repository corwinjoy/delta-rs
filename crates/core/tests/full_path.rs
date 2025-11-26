// Verifies that tables with fully-qualified file paths behave as expected
// and that reading them yields the same data as the corresponding table
// with relative file paths.

use std::fs;
use std::path::{Path, PathBuf};
use serde_json::Value;
use url::Url;

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn read_table_with_full_paths_and_compare() {
    use deltalake_core::arrow::array::RecordBatch;
    use deltalake_core::datafusion::assert_batches_sorted_eq;
    use deltalake_core::datafusion::common::test_util::format_batches;
    use deltalake_core::operations::collect_sendable_stream;
    use deltalake_core::DeltaOps;

    let expected_rel = Path::new("../test/tests/data/delta-0.8.0");
    let expected_abs = fs::canonicalize(expected_rel).unwrap();
    
    // Use relative paths and resolve them to absolute by canonicalizing.
    let full_path_rel = Path::new("../test/tests/data/delta-0.8.0-full-path");
    let full_path_abs: PathBuf = fs::canonicalize(full_path_rel).unwrap();

    clone_test_dir_with_abs_paths(&expected_abs, &full_path_abs);

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

fn clone_test_dir_with_abs_paths(src_dir: &PathBuf, target_dir: &PathBuf) {
    // Setup: ensure the test table at `full_path_rel` exists and its _delta_log
    // contains fully-qualified (absolute) file paths for all add actions.
    // 1) Replace any existing directory at `full_path_rel` with a fresh copy of
    //    `expected_rel`.
    // 2) Rewrite all add actions in JSON log files to use absolute file paths.
    {
        // Re-create target directory by copying from the expected (relative-path) table.
        if target_dir.exists() {
            fs::remove_dir_all(&target_dir).unwrap();
        }
        fs::create_dir_all(&target_dir).unwrap();

        // Copy contents of expected table into target directory.
        use fs_extra::dir::{copy as copy_dir, CopyOptions};
        let mut opts = CopyOptions::new();
        opts.overwrite = true;
        opts.copy_inside = true;
        opts.content_only = true; // copy contents of source into dest root
        copy_dir(&src_dir, &target_dir, &opts).unwrap();

        // Now, rewrite _delta_log entries so that add.path values are absolute.
        let log_dir = target_dir.join("_delta_log");
        for entry in fs::read_dir(&log_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let original = fs::read_to_string(&path).unwrap();
            let mut rewritten_lines: Vec<String> = Vec::new();
            for line in original.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
                if let Some(add) = v.get_mut("add") {
                    use_abs_path_for_action(target_dir, add);
                }
                if let Some(remove) = v.get_mut("remove") {
                    use_abs_path_for_action(target_dir, remove);
                }
                rewritten_lines.push(serde_json::to_string(&v).unwrap());
            }
            let new_contents = rewritten_lines.join("\n");
            fs::write(&path, new_contents).unwrap();
        }
    }
}

fn use_abs_path_for_action(target_dir: &PathBuf, action: &mut Value) {
    if let Some(obj) = action.as_object_mut() {
        if let Some(path_val) = obj.get_mut("path") {
            if let Some(rel) = path_val.as_str() {
                // Convert to absolute path under the copied table directory
                let abs = target_dir.join(rel);
                *path_val = serde_json::Value::String(
                    abs.to_str().unwrap().to_string(),
                );
            }
        }
    }
}
