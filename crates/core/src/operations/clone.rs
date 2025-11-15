//! Clone a Delta table from a source location into a target location by
//! creating a new table at the target and registering the source table's files.
use std::path::PathBuf;
use futures::TryStreamExt;
use url::Url;
use crate::kernel::{Action, Add, EagerSnapshot};
use crate::kernel::transaction::{CommitBuilder, TransactionError};
use crate::operations::create::CreateBuilder;
use crate::protocol::{DeltaOperation, SaveMode};
use crate::table::builder::DeltaTableBuilder;
use crate::{DeltaResult, DeltaTable};

/// Clone a Delta table by creating a new table at `target_url` and registering
/// all data files from the source table at `source_url`.
///
/// Note: This clones the table metadata and references to the data files. It does
/// not copy the underlying data files themselves.
pub async fn clone(source: Url, target: Url, version: Option<i64>) -> DeltaResult<DeltaTable> {
    // 1) Load source table and get snapshot/metadata
    let mut source_table = DeltaTableBuilder::from_uri(source)?
            .load()
            .await?;
    if let Some(v) = version {
        source_table.load_version(v).await?;
    }
    let source_state = source_table.snapshot()?;
    let source_snapshot: &EagerSnapshot = source_state.snapshot();
    let source_log = source_table.log_store();

    let src_metadata = source_state.metadata().clone();
    let src_schema = src_metadata.parse_schema().expect("valid source schema");
    let partition_columns = src_metadata.partition_columns().to_vec();
    let configuration = src_metadata.configuration().clone();
    let src_protocol = source_state.protocol().clone();

    // 2) Create target table with schema/partitions/config and source protocol
    //    (Use CreateBuilder to mirror CreateTable).
    let mut create = CreateBuilder::new()
        .with_location(target.as_ref().to_string())
        .with_columns(src_schema.fields().cloned())
        .with_partition_columns(partition_columns.clone())
        .with_configuration(configuration.iter().map(|(k, v)| (k.clone(), Some(v.clone()))));

    if let Some(n) = src_metadata.name() {
        create = create.with_table_name(n.to_string());
    }
    if let Some(d) = src_metadata.description() {
        create = create.with_comment(d.to_string());
    }

    // Ensure we align protocol with source table for compatibility.
    let mut target_table = create
        .with_actions([Action::Protocol(src_protocol.clone())])
        .await?;

    // 3) Gather source files represented by the snapshot
    let file_views: Vec<_> = source_snapshot
        .file_views(&source_log, None)
        .try_collect()
        .await?;

    let mut actions = Vec::with_capacity(file_views.len() + 1);
    // Add metadata from source snapshot explicitly
    actions.push(Action::Metadata(src_metadata.clone()));

    // 4) Add files to target table using Add actions (mirrors WriteBuilder logic)
    //    and, for local filesystems, create symlinks in the target table that point
    //    to the source data files.

    let target_root_path = target_table
        .log_store()
        .config()
        .location
        .to_file_path()
        .unwrap();

    let source_root_path = source_log
        .config()
        .location
        .to_file_path()
        .unwrap();

    for view in file_views.into_iter() {
        let mut add = view.add_action();
        // For now, it seems we have to use relative paths only and have to create symlinks.
        add.data_change = true;
        add_symlink(source_root_path.clone(), target_root_path.clone(), add.path.clone());
        actions.push(Action::Add(add));
    }

    // 5) Commit ADD operations to the target table
    // Use a Write operation metadata similar to WriteBuilder
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: if partition_columns.is_empty() {
            None
        } else {
            Some(partition_columns)
        },
        predicate: None,
    };

    let target_snapshot = target_table.snapshot()?.snapshot().clone();
    let prepared_commit = CommitBuilder::default()
        .with_actions(actions)
        .build(Some(&target_snapshot), target_table.log_store(), operation)
        .into_prepared_commit_future()
        .await?;

    // Write commit entry: next version
    let log_store = target_table.log_store();
    let commit_version = target_snapshot.version() + 1;
    let commit_bytes = prepared_commit.commit_or_bytes();
    let operation_id = uuid::Uuid::new_v4();
    match log_store
        .write_commit_entry(commit_version, commit_bytes.clone(), operation_id)
        .await
    {
        Ok(_) => {}
        Err(err @ TransactionError::VersionAlreadyExists(_)) => {
            return Err(err.into());
        }
        Err(err) => {
            // Abort and return error
            log_store
                .abort_commit_entry(commit_version, commit_bytes.clone(), operation_id)
                .await?;
            return Err(err.into());
        }
    }

    // Refresh target table to include new commit
    target_table.update().await?;
    Ok(target_table)
}

fn add_symlink(source_root_path: PathBuf, target_root_path: PathBuf, add_filename: String) {
    // Best-effort symlink creation: only when both source and target are local filesystems.
    let file_name = std::path::Path::new(&add_filename);
    let src_path = source_root_path.join(file_name);
    let link_path = target_root_path.join(file_name);

    // On Windows, use symlink_file; on Unix, use symlink.
    #[cfg(target_family = "windows")]
    {
        use std::os::windows::fs::symlink_file;
        let _ = symlink_file(&src_path, &link_path);
    }
    #[cfg(target_family = "unix")]
    {
        use std::os::unix::fs::symlink;
        let _ = symlink(&src_path, &link_path);
    }
}

#[cfg(all(test, feature = "datafusion"))]
mod tests {
use super::*;
    use std::path::Path;
    use arrow::array::RecordBatch;
    use url::Url;
    use crate::DeltaOps;
    use crate::operations::collect_sendable_stream;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::common::test_util::format_batches;

    #[tokio::test]
    async fn test_clone_operation() -> DeltaResult<()> {
        let source_path = Path::new("../test/tests/data/simple_table");
        let source_uri =
            Url::from_directory_path(std::fs::canonicalize(source_path)?).unwrap();

        let clone_path = Path::new("../test/tests/data/simple_table_clone");
        // Erase contents of clone_path directory to ensure a clean target
        if clone_path.exists() {
            std::fs::remove_dir_all(clone_path)?;
        }
        std::fs::create_dir_all(clone_path)?;
        let clone_uri =
            Url::from_directory_path(std::fs::canonicalize(clone_path)?).unwrap();

        let version = 3;
        let cloned_table = clone(source_uri.clone(), clone_uri.clone(), Some(version)).await?;

        let mut source_table = DeltaTableBuilder::from_uri(source_uri.clone())?
            .load()
            .await?;

        source_table.load_version(version).await?;

        let src_uris: Vec<_> = source_table.get_file_uris()?.collect();
        let cloned_uris: Vec<_> = cloned_table.get_file_uris()?.collect();
        
        // Convert URIs to just file names for comparison
        let mut src_files: Vec<String> = src_uris
            .into_iter()
            .filter_map(|url| Some(String::from(Path::new(&url).file_name()?.to_str()?)))
            .collect();
        let mut cloned_files: Vec<String> = cloned_uris
            .into_iter()
            .filter_map(|url| Some(String::from(Path::new(&url).file_name()?.to_str()?)))
            .collect();

        src_files.sort();
        cloned_files.sort();
        println!("Source files: {:#?}", src_files);
        println!("Cloned files: {:#?}", cloned_files);
        assert_eq!(src_files, cloned_files, "Cloned table should reference the same files as the source");

        let cloned_ops = DeltaOps::try_from_uri(clone_uri).await?;
        let (_table, stream) = cloned_ops.load().await?;
        let cloned_data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

        // println!("{cloned_data:?}");

        let pretty_cloned_data = format_batches(&*cloned_data)?.to_string();
        println!();
        println!("Cloned data:");
        println!("{pretty_cloned_data}");

        let mut src_ops = DeltaOps::try_from_uri(source_uri).await?;
        src_ops.0.load_version(version).await?;
        let (_table, stream) = src_ops.load().await?;
        let source_data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

        let expected_lines = format_batches(&*source_data)?.to_string();
        let expected_lines_vec: Vec<&str> = expected_lines.trim().lines().collect();

        assert_batches_sorted_eq!(&expected_lines_vec, &cloned_data);

        println!();
        println!("Source data:");
        println!("{expected_lines}");
        Ok(())
    }
}