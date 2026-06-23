//! DataFusion-free data-file readers (file and dataset tiers).
//!
//! [`DataFileReader`] is the per-file seam — the symmetric counterpart to
//! [`super::DataFileWriter`] — where parquet read properties (in the future,
//! `FileDecryptionProperties`) attach. [`KernelDataReader`] is the dataset-tier
//! [`DeltaDataReader`]; it is intended to compose a [`DataFileReader`] over the
//! files selected by `delta-kernel`'s scan, which already reads parquet directly
//! and applies deletion vectors, partition values, and column-mapping transforms.
//!
//! Both implementations land in a later phase; for now their methods return a
//! clear "not yet implemented" error so callers that need DataFusion-free reads
//! fail loudly rather than silently falling back. Reads in a `datafusion`-enabled
//! build go through [`crate::datafile::ext::DeltaDataReaderExt`].

use object_store::path::Path;

use crate::errors::{DeltaResult, DeltaTableError};

use super::{DeltaDataReader, ReadOptions, RecordBatchFutureStream};

/// File tier: reads a single parquet data file into a stream of record batches.
///
/// This is where parquet read / decryption properties attach, mirroring
/// [`super::DataFileWriter`] on the write side.
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

/// File-tier reader backed by `delta-kernel`'s parquet handler.
///
/// Placeholder: the kernel-backed implementation is a follow-up (see module docs).
#[derive(Debug, Clone, Default)]
pub struct KernelDataFileReader {
    _private: (),
}

#[async_trait::async_trait]
impl DataFileReader for KernelDataFileReader {
    async fn read_file(&self, _path: Path) -> DeltaResult<RecordBatchFutureStream> {
        Err(not_yet_implemented("KernelDataFileReader"))
    }
}

/// Dataset tier: a DataFusion-free reader backed by `delta-kernel`'s scan engine.
///
/// Currently a placeholder: the kernel-backed implementation (composing
/// [`KernelDataFileReader`] over the kernel scan) is a follow-up.
#[derive(Debug, Clone, Default)]
pub struct KernelDataReader {
    _private: (),
}

impl KernelDataReader {
    /// Create a new [`KernelDataReader`].
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl DeltaDataReader for KernelDataReader {
    async fn read(&self, _options: ReadOptions) -> DeltaResult<RecordBatchFutureStream> {
        Err(not_yet_implemented("KernelDataReader"))
    }
}
