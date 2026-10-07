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
/// Async so implementations can fetch per-file keys from a KMS. Plain [`WriterProperties`]
/// hand out the same properties for every file.
///
/// # File paths and AAD
/// File paths are relative to the table root, as in the Delta log, and readers pass
/// decryption factories the same paths. A factory should use only the file name as the AAD
/// prefix (additional authenticated data), which binds encrypted modules to their file:
/// file names are unique within a table and, unlike full paths, survive moving it. The
/// reference `KmsEncryptionFactory` does this.
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

    /// A factory like this one, but with `properties` as the base settings (compression,
    /// row groups, statistics). Anything the factory adds on top, such as encryption, is
    /// kept, so callers' settings cannot turn it off.
    ///
    /// The default returns `None`: the factory cannot take new base settings and is used
    /// unchanged (see [`with_base_properties`]), which ignores the caller's settings but
    /// never loses what the factory adds.
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

    fn with_base_properties(
        &self,
        properties: WriterProperties,
    ) -> Option<WriterPropertiesFactoryRef> {
        Some(factory_from_writer_properties(properties))
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
    fn base_properties(&self) -> &WriterProperties {
        self.inner.base_properties()
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
        base: WriterProperties,
        paths: Arc<Mutex<Vec<Path>>>,
    }

    #[async_trait]
    impl WriterPropertiesFactory for RecordingFactory {
        fn base_properties(&self) -> &WriterProperties {
            &self.base
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
