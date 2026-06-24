//! DataFusion-free data-file readers (file and dataset tiers).
//!
//! This module hosts two kinds of reader:
//!
//! * [`ParquetFileReader`] / [`ParquetTableReader`] — **new in this PR.** The
//!   first concrete implementations of the read traits, added to prove the read
//!   side of the design end-to-end (the write side already has real impls).
//!   They read raw parquet directly from object storage with no DataFusion, and
//!   are intentionally minimal: they reject tables that need deletion-vector
//!   application, column mapping, or partition-value reconstruction.
//! * [`KernelDataFileReader`] / [`KernelDataReader`] — placeholders for the
//!   later, full-fidelity reader backed by `delta-kernel`'s scan engine (which
//!   applies deletion vectors, partition values, and column-mapping transforms).
//!
//! In a `datafusion` build, full reads go through
//! [`crate::datafile::ext::DeltaDataReaderExt`].

use std::sync::Arc;

use delta_kernel::table_features::ColumnMappingMode;
use futures::stream::{StreamExt as _, TryStreamExt as _};
use object_store::ObjectStore;
use object_store::path::Path;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};

use crate::DeltaTable;
use crate::errors::{DeltaResult, DeltaTableError};

use super::{BatchFuture, DeltaDataReader, ReadOptions, RecordBatchFutureStream};

/// File tier: reads a single parquet data file (the per-file decryption seam,
/// mirroring [`super::DataFileWriter`]).
#[async_trait::async_trait]
pub trait DataFileReader: Send + Sync {
    /// Read the parquet data file at `path` into a stream of record batches.
    async fn read_file(&self, path: Path) -> DeltaResult<RecordBatchFutureStream>;
}

fn not_yet_implemented(what: &str) -> DeltaTableError {
    DeltaTableError::Generic(format!(
        "The DataFusion-free read path ({what}) is not yet implemented. \
         Enable the `datafusion` feature and use DeltaDataReaderExt for reads."
    ))
}

fn not_supported(feature: &str) -> DeltaTableError {
    DeltaTableError::Generic(format!(
        "ParquetTableReader cannot read a table that uses {feature}; \
         use the DataFusion read path (DeltaDataReaderExt) for such tables."
    ))
}

// ---------------------------------------------------------------------------
// New: a concrete, DataFusion-free parquet reader that proves the read traits.
// ---------------------------------------------------------------------------

/// File-tier reader that reads a single parquet data file directly from object
/// storage, with no DataFusion.
///
/// New in this PR: the first concrete [`DataFileReader`], added to validate the
/// per-file read seam (the same seam where parquet decryption will later
/// attach, mirroring the write side).
#[derive(Debug, Clone)]
pub struct ParquetFileReader {
    store: Arc<dyn ObjectStore>,
}

impl ParquetFileReader {
    /// Create a reader over the given object store.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl DataFileReader for ParquetFileReader {
    async fn read_file(&self, path: Path) -> DeltaResult<RecordBatchFutureStream> {
        let reader = ParquetObjectReader::new(self.store.clone(), path);
        let stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await?
            .build()?;
        // Wrap each parquet batch in a ready future, matching the
        // `ext::sendable_to_future_stream` adapter shape on the DataFusion side.
        Ok(stream
            .map(|res| -> BatchFuture {
                Box::pin(async move { res.map_err(DeltaTableError::from) })
            })
            .boxed())
    }
}

/// Dataset-tier reader that reads all of a table's parquet data files directly
/// (no DataFusion, no predicate), composing [`ParquetFileReader`] across the
/// table.
///
/// New in this PR: the first concrete [`DeltaDataReader`], added to prove that
/// the file tier composes into a whole-table read through the
/// [`RecordBatchFutureStream`] waist.
///
/// Minimal by design — it reads raw parquet, so [`ParquetTableReader::try_new`]
/// rejects tables that use deletion vectors, column mapping, or partition
/// columns. Honoring those is the job of the kernel-backed [`KernelDataReader`].
#[derive(Debug, Clone)]
pub struct ParquetTableReader {
    file_reader: ParquetFileReader,
    paths: Vec<Path>,
}

impl ParquetTableReader {
    /// Build a reader over the table's current data files.
    ///
    /// Errors if the table uses a feature this raw reader cannot honor
    /// (deletion vectors, column mapping, or partition columns).
    pub async fn try_new(table: &DeltaTable) -> DeltaResult<Self> {
        let snapshot = table.snapshot()?;

        // Guard: column mapping would mean physical (not logical) column names.
        if snapshot
            .table_config()
            .column_mapping_mode
            .is_some_and(|mode| mode != ColumnMappingMode::None)
        {
            return Err(not_supported("column mapping"));
        }
        // Guard: partition column values live in the path, not the parquet file.
        if !snapshot.metadata().partition_columns().is_empty() {
            return Err(not_supported("partition columns"));
        }

        let log_store = table.log_store();
        let mut paths = Vec::new();
        let mut views = snapshot.snapshot().file_views(log_store.as_ref(), None);
        while let Some(view) = views.try_next().await? {
            // Guard: a raw read would return rows that a deletion vector removes.
            if view.deletion_vector_descriptor().is_some() {
                return Err(not_supported("deletion vectors"));
            }
            paths.push(Path::from(view.path().as_ref()));
        }

        let store = log_store.object_store(None);
        Ok(Self {
            file_reader: ParquetFileReader::new(store),
            paths,
        })
    }
}

#[async_trait::async_trait]
impl DeltaDataReader for ParquetTableReader {
    async fn read(&self, options: ReadOptions) -> DeltaResult<RecordBatchFutureStream> {
        // Projection / limit pushdown is a follow-up; reject rather than silently
        // ignore so callers don't get more data than they asked for.
        if options.projection.is_some() || options.limit.is_some() {
            return Err(DeltaTableError::Generic(
                "projection and limit are not yet supported by the basic parquet reader".into(),
            ));
        }

        let file_reader = self.file_reader.clone();
        let stream = futures::stream::iter(self.paths.clone())
            .then(move |path| {
                let file_reader = file_reader.clone();
                async move { file_reader.read_file(path).await }
            })
            // Flatten each file's batch stream into one; surface an open error as
            // a single failing batch future so it is not silently dropped.
            .map(|opened| -> RecordBatchFutureStream {
                match opened {
                    Ok(file_stream) => file_stream,
                    Err(err) => {
                        let failing: BatchFuture = Box::pin(async move { Err(err) });
                        futures::stream::once(async move { failing }).boxed()
                    }
                }
            })
            .flatten()
            .boxed();
        Ok(stream)
    }
}

// ---------------------------------------------------------------------------
// Placeholders for the future kernel-backed (full-fidelity) reader.
// ---------------------------------------------------------------------------

/// File-tier reader backed by `delta-kernel`'s parquet handler (placeholder).
#[derive(Debug, Clone, Default)]
pub struct KernelDataFileReader;

#[async_trait::async_trait]
impl DataFileReader for KernelDataFileReader {
    async fn read_file(&self, _path: Path) -> DeltaResult<RecordBatchFutureStream> {
        Err(not_yet_implemented("KernelDataFileReader"))
    }
}

/// Dataset-tier reader backed by `delta-kernel`'s scan engine (placeholder).
///
/// Unlike [`ParquetTableReader`], this will apply deletion vectors, partition
/// values, and column-mapping transforms — the full Delta read semantics.
#[derive(Debug, Clone, Default)]
pub struct KernelDataReader;

#[async_trait::async_trait]
impl DeltaDataReader for KernelDataReader {
    async fn read(&self, _options: ReadOptions) -> DeltaResult<RecordBatchFutureStream> {
        Err(not_yet_implemented("KernelDataReader"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::create::CreateBuilder;
    use crate::writer::DeltaWriter as _;
    use crate::writer::RecordBatchWriter;
    use crate::writer::test_utils::{get_delta_schema, get_record_batch};

    #[tokio::test]
    async fn test_parquet_table_reader_reads_all_files() {
        // Prove the read traits end-to-end: write a plain (unpartitioned, no-DV,
        // no-column-mapping) table, then read every parquet file back with no
        // predicate through the DataFusion-free reader.
        let schema = get_delta_schema();
        let tmp = tempfile::tempdir().unwrap();
        let mut table = CreateBuilder::new()
            .with_location(tmp.path().to_str().unwrap())
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();

        let batch = get_record_batch(None, false);
        let expected_rows = batch.num_rows();
        let mut writer = RecordBatchWriter::for_table(&table).unwrap();
        writer.write(batch).await.unwrap();
        writer.flush_and_commit(&mut table).await.unwrap();

        let reader = ParquetTableReader::try_new(&table).await.unwrap();
        let stream = reader.read(ReadOptions::default()).await.unwrap();
        let batches: Vec<_> = stream.buffered(4).try_collect().await.unwrap();

        assert!(!batches.is_empty(), "expected at least one batch");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, expected_rows);
    }

    #[tokio::test]
    async fn test_parquet_table_reader_rejects_partitioned_table() {
        // The raw reader cannot reconstruct partition columns, so it must refuse
        // a partitioned table rather than return rows missing those columns.
        let schema = get_delta_schema();
        let tmp = tempfile::tempdir().unwrap();
        let table = CreateBuilder::new()
            .with_location(tmp.path().to_str().unwrap())
            .with_columns(schema.fields().cloned())
            .with_partition_columns(vec!["modified".to_string()])
            .await
            .unwrap();

        let err = ParquetTableReader::try_new(&table).await.unwrap_err();
        assert!(matches!(err, DeltaTableError::Generic(_)), "got: {err:?}");
    }
}
