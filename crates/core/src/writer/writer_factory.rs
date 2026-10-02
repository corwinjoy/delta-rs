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
use parquet::schema::types::ColumnPath;

use crate::errors::DeltaResult;
use crate::parquet_utils::default_writer_properties;

/// Creates the [`WriterProperties`] for each Parquet file.
///
/// It is async so implementations can fetch per-file keys from a KMS, using the file path
/// as additional authenticated data (AAD).
#[async_trait]
pub trait WriterPropertiesFactory: Send + Sync + Debug + 'static {
    /// The compression for `column_path`; the writer uses it to pick the file extension
    /// before any properties are created.
    fn compression(&self, column_path: &ColumnPath) -> Compression;

    /// The [`WriterProperties`] for a new file, called once just before it is opened.
    /// Implementations using AAD must derive keys from `file_path`.
    async fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties>;
}

/// Shared handle to a [`WriterPropertiesFactory`].
pub type WriterPropertiesFactoryRef = Arc<dyn WriterPropertiesFactory>;

/// A [`WriterPropertiesFactory`] that returns the same [`WriterProperties`] for every file.
#[derive(Clone, Debug)]
pub struct DefaultWriterPropertiesFactory {
    writer_properties: WriterProperties,
}

impl DefaultWriterPropertiesFactory {
    /// Create a factory that hands out `writer_properties` for every file.
    pub fn new(writer_properties: WriterProperties) -> Self {
        Self { writer_properties }
    }

    /// Create a factory using SNAPPY compression and the delta-rs `created_by` tag.
    pub fn snappy() -> Self {
        Self::new(snappy_writer_properties())
    }
}

/// The default delta-rs [`WriterProperties`]: SNAPPY compression and the delta-rs
/// `created_by` tag.
pub fn snappy_writer_properties() -> WriterProperties {
    default_writer_properties(Compression::SNAPPY)
}

#[async_trait]
impl WriterPropertiesFactory for DefaultWriterPropertiesFactory {
    fn compression(&self, column_path: &ColumnPath) -> Compression {
        self.writer_properties.compression(column_path)
    }

    async fn create_writer_properties(
        &self,
        _file_path: &Path,
        _file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        Ok(self.writer_properties.clone())
    }
}

/// A factory for the default delta-rs properties (SNAPPY, no encryption).
pub fn default_writer_properties_factory() -> WriterPropertiesFactoryRef {
    Arc::new(DefaultWriterPropertiesFactory::snappy())
}

/// A factory that returns `wp` for every file.
pub fn factory_from_writer_properties(wp: WriterProperties) -> WriterPropertiesFactoryRef {
    Arc::new(DefaultWriterPropertiesFactory::new(wp))
}
