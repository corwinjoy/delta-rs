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
/// It is async so implementations can fetch per-file keys from a KMS.
///
/// # File paths and AAD
/// File paths are relative to the table root, as in the Delta log's `add` and `cdc`
/// actions, and readers pass decryption factories the same paths. A factory should use only
/// the file name for the AAD prefix (additional authenticated data), which binds encrypted
/// modules to their file
/// (<https://parquet.apache.org/docs/file-format/data-pages/encryption/>). File names are
/// unique within a table and, unlike full paths, survive moving the table, so its files stay
/// readable. The KMS decides; `test_utils::kms_encryption::MockKmsFactory` uses the file name.
#[async_trait]
pub trait WriterPropertiesFactory: Send + Sync + Debug + 'static {
    /// The compression for `column_path`; the writer uses it to pick the file extension
    /// before any properties are created.
    fn compression(&self, column_path: &ColumnPath) -> Compression;

    /// The row-group row limit of the base properties, if any; the writer slices batches
    /// at row-group boundaries before any properties are created.
    fn max_row_group_row_count(&self) -> Option<usize> {
        None
    }

    /// The row-group byte limit of the base properties, if any.
    fn max_row_group_bytes(&self) -> Option<usize> {
        None
    }

    /// The [`WriterProperties`] for a new file, called once just before it is opened.
    async fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties>;

    /// A factory like this one, but with `properties` as the base settings (compression,
    /// row groups, statistics). Anything the factory adds on top, such as encryption, is
    /// kept, so callers' settings cannot turn it off.
    ///
    /// The default returns `None`: the factory cannot take new base settings, and writers
    /// keep using it unchanged (see [`with_base_properties`]). That ignores the caller's
    /// settings but never loses what the factory adds, so implementations that do not
    /// override this stay safe.
    fn with_base_properties(
        &self,
        properties: WriterProperties,
    ) -> Option<WriterPropertiesFactoryRef> {
        let _ = properties;
        None
    }
}

/// Shared handle to a [`WriterPropertiesFactory`].
pub type WriterPropertiesFactoryRef = Arc<dyn WriterPropertiesFactory>;

/// `factory` with `properties` as its base settings, or `factory` itself when it cannot take
/// new base settings (see [`WriterPropertiesFactory::with_base_properties`]).
pub fn with_base_properties(
    factory: &WriterPropertiesFactoryRef,
    properties: WriterProperties,
) -> WriterPropertiesFactoryRef {
    factory
        .with_base_properties(properties)
        .unwrap_or_else(|| Arc::clone(factory))
}

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

    fn max_row_group_row_count(&self) -> Option<usize> {
        self.writer_properties.max_row_group_row_count()
    }

    fn max_row_group_bytes(&self) -> Option<usize> {
        self.writer_properties.max_row_group_bytes()
    }

    async fn create_writer_properties(
        &self,
        _file_path: &Path,
        _file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        Ok(self.writer_properties.clone())
    }

    fn with_base_properties(
        &self,
        properties: WriterProperties,
    ) -> Option<WriterPropertiesFactoryRef> {
        Some(factory_from_writer_properties(properties))
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

/// A factory for files written below `prefix` through a store rooted there, such as
/// `_change_data`: it passes `factory` the table-relative path (`_change_data/part-….parquet`)
/// rather than the path within that store.
pub(crate) fn with_path_prefix(
    factory: WriterPropertiesFactoryRef,
    prefix: &str,
) -> WriterPropertiesFactoryRef {
    Arc::new(PrefixedWriterPropertiesFactory {
        prefix: Path::from(prefix),
        inner: factory,
    })
}

#[derive(Debug)]
struct PrefixedWriterPropertiesFactory {
    prefix: Path,
    inner: WriterPropertiesFactoryRef,
}

#[async_trait]
impl WriterPropertiesFactory for PrefixedWriterPropertiesFactory {
    fn compression(&self, column_path: &ColumnPath) -> Compression {
        self.inner.compression(column_path)
    }

    fn max_row_group_row_count(&self) -> Option<usize> {
        self.inner.max_row_group_row_count()
    }

    fn max_row_group_bytes(&self) -> Option<usize> {
        self.inner.max_row_group_bytes()
    }

    async fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        let table_path = Path::from_iter(self.prefix.parts().chain(file_path.parts()));
        self.inner
            .create_writer_properties(&table_path, file_schema)
            .await
    }

    fn with_base_properties(
        &self,
        properties: WriterProperties,
    ) -> Option<WriterPropertiesFactoryRef> {
        Some(Arc::new(Self {
            prefix: self.prefix.clone(),
            inner: with_base_properties(&self.inner, properties),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use arrow_schema::Schema as ArrowSchema;

    use super::*;

    /// Records the paths it is asked to create properties for.
    #[derive(Debug, Default)]
    struct RecordingFactory {
        paths: Arc<Mutex<Vec<Path>>>,
    }

    #[async_trait]
    impl WriterPropertiesFactory for RecordingFactory {
        fn compression(&self, _column_path: &ColumnPath) -> Compression {
            Compression::SNAPPY
        }

        async fn create_writer_properties(
            &self,
            file_path: &Path,
            _file_schema: &Arc<ArrowSchema>,
        ) -> DeltaResult<WriterProperties> {
            self.paths.lock().unwrap().push(file_path.clone());
            Ok(snappy_writer_properties())
        }
    }

    /// A factory that does not override `with_base_properties` is used unchanged.
    #[tokio::test]
    async fn default_with_base_properties_keeps_the_factory() {
        let factory: WriterPropertiesFactoryRef = Arc::new(RecordingFactory::default());
        let rebased = with_base_properties(&factory, snappy_writer_properties());
        assert!(Arc::ptr_eq(&factory, &rebased));
        assert!(
            factory
                .with_base_properties(snappy_writer_properties())
                .is_none()
        );
    }

    #[tokio::test]
    async fn path_prefix_passes_table_relative_paths() {
        let recording = RecordingFactory::default();
        let paths = Arc::clone(&recording.paths);
        let factory = with_path_prefix(Arc::new(recording), "_change_data")
            .with_base_properties(snappy_writer_properties())
            .expect("the prefixed factory takes base properties");
        let schema = Arc::new(ArrowSchema::empty());
        factory
            .create_writer_properties(&Path::from("year=2024/part-0.parquet"), &schema)
            .await
            .unwrap();
        assert_eq!(
            *paths.lock().unwrap(),
            [Path::from("_change_data/year=2024/part-0.parquet")]
        );
    }
}
