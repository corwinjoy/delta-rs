//! Clone a Delta table from a source location into a target location by
//! creating a new table at the target and registering the source table's files.
//!
//! Steps:
//! 1. Create a new table at the clone location (mirrors CreateTable logic).
//! 2. Copy metadata from the source snapshot (mirrors AddColumnBuilder metadata handling).
//! 3. List files represented by the source snapshot.
//! 4. Convert file paths to absolute URIs.
//! 5. Add files to the target table using Add actions (mirrors WriteBuilder logic).

use futures::TryStreamExt;
use object_store::path::Path;

use crate::kernel::{Action, EagerSnapshot};
use crate::kernel::transaction::{CommitBuilder, TransactionError};
use crate::operations::create::CreateBuilder;
use crate::protocol::{DeltaOperation, SaveMode};
use crate::table::builder::{ensure_table_uri, DeltaTableBuilder};
use crate::{DeltaResult, DeltaTable};

/// Clone a Delta table by creating a new table at `target_url` and registering
/// all data files from the source table at `source_url`.
///
/// Note: This clones the table metadata and references to the data files. It does
/// not copy the underlying data files themselves.
pub async fn clone(source_url: impl AsRef<str>, target_url: impl AsRef<str>) -> DeltaResult<DeltaTable> {
    // 1) Load source table and get snapshot/metadata
    let source_table = DeltaTableBuilder::from_uri(ensure_table_uri(source_url.as_ref())?)?
            .load()
            .await?;
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
        .with_location(target_url.as_ref().to_string())
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

    // 4) Convert relative paths to absolute URIs (source rooted)
    let mut actions = Vec::with_capacity(file_views.len() + 1);
    // Add metadata from source snapshot explicitly
    actions.push(Action::Metadata(src_metadata.clone()));
    actions.extend(file_views.into_iter().map(|view| {
        let mut add = view.add_action();
        // Absolutize using the source log store root
        let abs_uri = source_log.to_uri(&Path::from(add.path.clone()));
        add.path = abs_uri;
        add.data_change = true; // explicit, mirrors WriteBuilder
        Action::Add(add)
    }));

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
