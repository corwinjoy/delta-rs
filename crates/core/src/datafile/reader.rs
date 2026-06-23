//! DataFusion-free data-file readers (file and dataset tiers).
//!
//! Placeholders for a later phase — methods return "not yet implemented". In a
//! `datafusion` build, reads go through [`crate::datafile::ext::DeltaDataReaderExt`].

use object_store::path::Path;

use crate::errors::{DeltaResult, DeltaTableError};

use super::{DeltaDataReader, ReadOptions, RecordBatchFutureStream};

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
#[derive(Debug, Clone, Default)]
pub struct KernelDataReader;

#[async_trait::async_trait]
impl DeltaDataReader for KernelDataReader {
    async fn read(&self, _options: ReadOptions) -> DeltaResult<RecordBatchFutureStream> {
        Err(not_yet_implemented("KernelDataReader"))
    }
}
