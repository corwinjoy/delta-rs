//! Per-file [`WriterProperties`] for Parquet writers.
//!
//! Free of `datafusion` dependencies, so the legacy writers (`JsonWriter`,
//! `RecordBatchWriter`) can use it without the `datafusion` feature.

use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use object_store::path::Path;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::errors::DeltaResult;
use crate::parquet_utils::default_writer_properties;

/// Creates the [`WriterProperties`] for each Parquet file.
///
/// Async so implementations can fetch per-file keys from a KMS, using the file path as
/// additional authenticated data (AAD). Plain [`WriterProperties`] hand out the same
/// properties for every file.
#[async_trait]
pub trait WriterPropertiesFactory: Send + Sync + Debug + 'static {
    /// The base settings every file is written with (compression, row-group limits).
    /// Writers read them before any file's own properties are created.
    fn base_properties(&self) -> &WriterProperties;

    /// The [`WriterProperties`] for a new file, called once just before it is opened.
    async fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties>;
}

/// Shared handle to a [`WriterPropertiesFactory`].
pub type WriterPropertiesFactoryRef = Arc<dyn WriterPropertiesFactory>;

/// Fixed properties for every file: the factory for unencrypted tables.
#[async_trait]
impl WriterPropertiesFactory for WriterProperties {
    fn base_properties(&self) -> &WriterProperties {
        self
    }

    async fn create_writer_properties(
        &self,
        _file_path: &Path,
        _file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        Ok(self.clone())
    }
}

/// The default delta-rs [`WriterProperties`]: SNAPPY compression and the delta-rs
/// `created_by` tag.
pub fn snappy_writer_properties() -> WriterProperties {
    default_writer_properties(Compression::SNAPPY)
}

/// A factory for the default delta-rs properties (SNAPPY, no encryption).
pub fn default_writer_properties_factory() -> WriterPropertiesFactoryRef {
    Arc::new(snappy_writer_properties())
}

/// A factory that returns `wp` for every file.
pub fn factory_from_writer_properties(wp: WriterProperties) -> WriterPropertiesFactoryRef {
    Arc::new(wp)
}
