//! Per-table file format options for customising Parquet reads and writes.
//!
//! The central abstraction is [`FileFormatOptions`], an `Arc`-able trait that bundles:
//!
//! - A [`WriterPropertiesFactory`] for creating per-file [`WriterProperties`] (enabling
//!   KMS-based per-file encryption key generation).
//! - A DataFusion [`TableOptions`] snapshot used to configure parquet scan options (e.g.
//!   static `FileEncryptionProperties` or crypto factory references for reads).
//! - An [`update_session`](FileFormatOptions::update_session) hook that registers any
//!   necessary runtime state (e.g. a DataFusion [`EncryptionFactory`]) in a DataFusion
//!   [`Session`].
//!
//! ## How the options flow
//!
//! 1. The caller stores a `FileFormatRef` in [`DeltaTableConfig`](crate::table::builder::DeltaTableConfig)
//!    via [`DeltaTableBuilder::with_file_format_options`](crate::table::builder::DeltaTableBuilder::with_file_format_options).
//! 2. Each write operation builder receives the table config via `with_table_config` and
//!    calls [`apply_file_format_to_state`] after resolving its DataFusion session.  This
//!    function (a) invokes `update_session` (registering factories in the `RuntimeEnv`) and
//!    (b) stores the [`WriterPropertiesFactory`] as a [`DeltaWriterExtension`] inside the
//!    session's [`ConfigOptions`](datafusion::config::ConfigOptions).
//! 3. Write execution functions retrieve the factory from the session via
//!    [`factory_from_session`] and use it through [`WriterPropertiesFactory::create_writer_properties`].
//! 4. Read operations (`DeltaScan::scan`) call `update_session` directly and use the stored
//!    [`TableParquetOptions`] (set on `DeltaScanConfig` from the snapshot) for parquet source
//!    configuration.

use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::config::{
    ConfigEntry, ConfigExtension, EncryptionFactoryOptions, ExtensionOptions,
};
pub use datafusion::config::{TableOptions, TableParquetOptions};
use datafusion::execution::parquet_encryption::EncryptionFactory;
use datafusion::execution::{SessionState, SessionStateBuilder};
use object_store::path::Path;
use parquet::basic::Compression;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::file::properties::WriterProperties;
use parquet::file::properties::WriterPropertiesBuilder;
use parquet::schema::types::ColumnPath;
use tracing::debug;
use uuid::Uuid;

use crate::{DeltaResult, DeltaTableError, crate_version};

// ---------------------------------------------------------------------------
// FileFormatOptions trait
// ---------------------------------------------------------------------------

/// Top-level trait for per-table file format configuration.
///
/// Implement this trait and store an `Arc<dyn FileFormatOptions>` in
/// [`DeltaTableConfig`](crate::table::builder::DeltaTableConfig) to customise how the
/// table's Parquet files are read and written.
pub trait FileFormatOptions: Send + Sync + Debug + 'static {
    /// Returns DataFusion [`TableOptions`] representing the parquet configuration for this
    /// table (used when configuring parquet scans).
    fn table_options(&self) -> TableOptions;

    /// Returns a factory that creates [`WriterProperties`] for each new parquet file.
    fn writer_properties_factory(&self) -> WriterPropertiesFactoryRef;

    /// Register any runtime state (e.g. encryption factories) in the DataFusion session.
    ///
    /// The default implementation is a no-op — only KMS-style implementations need to
    /// override this.
    fn update_session(&self, _session: &dyn Session) -> DeltaResult<()> {
        Ok(())
    }
}

/// Convenience type alias for a reference-counted [`FileFormatOptions`].
pub type FileFormatRef = Arc<dyn FileFormatOptions>;

/// Convenience type alias for a reference-counted [`WriterPropertiesFactory`].
pub type WriterPropertiesFactoryRef = Arc<dyn WriterPropertiesFactory>;

// ---------------------------------------------------------------------------
// WriterPropertiesFactory trait
// ---------------------------------------------------------------------------

/// Async factory for creating per-file [`WriterProperties`].
///
/// The async signature allows implementations to fetch per-file encryption keys from a
/// remote KMS based on the file path and/or schema.
#[async_trait]
pub trait WriterPropertiesFactory: Send + Sync + Debug + 'static {
    /// Returns the compression used for unnamed columns — needed synchronously for
    /// building the file name before any async key fetch.
    fn compression(&self, column_path: &ColumnPath) -> Compression;

    /// Create [`WriterProperties`] for the given `file_path` and `file_schema`.
    ///
    /// This is called once per new parquet file, just before the `AsyncArrowWriter` is
    /// constructed.
    async fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties>;
}

// ---------------------------------------------------------------------------
// DeltaWriterExtension — session config extension that carries the factory
// ---------------------------------------------------------------------------

/// A DataFusion [`ConfigOptions`](datafusion::config::ConfigOptions) extension that stores
/// the active [`WriterPropertiesFactory`].
///
/// Written into the session by [`apply_file_format_to_state`] so that write execution
/// functions can retrieve it via [`factory_from_session`] without requiring callers to
/// thread the factory through every intermediate function signature.
#[derive(Clone, Debug, Default)]
pub struct DeltaWriterExtension {
    pub factory: Option<WriterPropertiesFactoryRef>,
}

impl ExtensionOptions for DeltaWriterExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, _key: &str, _value: &str) -> datafusion::error::Result<()> {
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}

impl ConfigExtension for DeltaWriterExtension {
    const PREFIX: &'static str = "delta.writer";
}

// ---------------------------------------------------------------------------
// Session helpers
// ---------------------------------------------------------------------------

/// Retrieve the [`WriterPropertiesFactory`] stored in the session by
/// [`apply_file_format_to_state`], if any.
pub fn factory_from_session(session: &dyn Session) -> Option<WriterPropertiesFactoryRef> {
    session
        .config_options()
        .extensions
        .get::<DeltaWriterExtension>()
        .and_then(|e| e.factory.clone())
}

/// Apply `file_format_options` to a [`SessionState`], returning a new state that:
///
/// 1. Has the encryption factory registered in the `RuntimeEnv` (via `update_session`).
/// 2. Carries the [`WriterPropertiesFactory`] as a [`DeltaWriterExtension`] in the session
///    config so write execution functions can retrieve it without explicit threading.
///
/// When `ffo` is `None` the original state is returned unchanged.
pub fn apply_file_format_to_state(
    state: SessionState,
    ffo: Option<&FileFormatRef>,
) -> DeltaResult<SessionState> {
    let Some(ffo) = ffo else {
        return Ok(state);
    };

    // Register encryption factory (and any other runtime state) in the RuntimeEnv.
    ffo.update_session(&state)?;

    // Store the writer factory in the session config extension so write functions can
    // retrieve it without needing it threaded through their signatures.
    let factory = ffo.writer_properties_factory();
    let mut config = state.config().clone();
    config
        .options_mut()
        .extensions
        .insert(DeltaWriterExtension {
            factory: Some(factory),
        });

    Ok(SessionStateBuilder::new_from_existing(state)
        .with_config(config)
        .build())
}

// ---------------------------------------------------------------------------
// SimpleFileFormatOptions — thin wrapper around TableOptions
// ---------------------------------------------------------------------------

/// A [`FileFormatOptions`] implementation that wraps a DataFusion [`TableOptions`].
///
/// Suitable for static encryption configurations (e.g. `FileEncryptionProperties`
/// baked directly into the parquet options) where no per-file key fetching is needed.
///
/// When `table_options.parquet.crypto.file_decryption` is set, this automatically
/// registers a [`StaticEncryptionFactory`] in the DataFusion `RuntimeEnv` using a
/// generated `factory_id`, so the encryption path works identically to the KMS case.
/// The `file_decryption` field is cleared from the scan-facing `table_options` so the
/// opener uses the factory path (which is reliable) rather than the
/// `ConfigFileDecryptionProperties` round-trip (which is unreliable in DataFusion v52).
#[derive(Clone, Debug)]
pub struct SimpleFileFormatOptions {
    table_options: TableOptions,
    /// Stable factory ID used to register static decryption in the RuntimeEnv.
    factory_id: Option<String>,
    /// The original `FileDecryptionProperties` extracted from the user's `TableOptions`,
    /// kept here because we clear `table_options.parquet.crypto.file_decryption` to force
    /// the factory path.
    decryption_properties: Option<Arc<FileDecryptionProperties>>,
}

impl Default for SimpleFileFormatOptions {
    fn default() -> Self {
        Self {
            table_options: TableOptions::default(),
            factory_id: None,
            decryption_properties: None,
        }
    }
}

impl SimpleFileFormatOptions {
    pub fn new(table_options: TableOptions) -> Self {
        let decryption_properties = table_options
            .parquet
            .crypto
            .file_decryption
            .clone()
            .map(|cfg| Arc::new(FileDecryptionProperties::from(cfg)));
        let factory_id = decryption_properties
            .as_ref()
            .map(|_| format!("delta-static-{}", Uuid::new_v4()));
        let mut opts = Self {
            table_options,
            factory_id,
            decryption_properties,
        };
        if let Some(id) = &opts.factory_id.clone() {
            // Set factory_id so scans look up the factory from RuntimeEnv.
            opts.table_options.parquet.crypto.factory_id = Some(id.clone());
            // Clear file_decryption so the opener uses the factory (not the broken
            // ConfigFileDecryptionProperties round-trip).
            opts.table_options.parquet.crypto.file_decryption = None;
        }
        opts
    }
}

impl FileFormatOptions for SimpleFileFormatOptions {
    fn table_options(&self) -> TableOptions {
        self.table_options.clone()
    }

    fn writer_properties_factory(&self) -> WriterPropertiesFactoryRef {
        match build_writer_properties_from_tpo(&Some(self.table_options.parquet.clone())) {
            Ok(Some(wp)) => Arc::new(SimpleWriterPropertiesFactory::new(wp)),
            Ok(None) => unreachable!("called with Some(parquet_options), result cannot be None"),
            Err(e) => Arc::new(ErrWriterPropertiesFactory {
                error: e.to_string(),
            }),
        }
    }

    fn update_session(&self, session: &dyn Session) -> DeltaResult<()> {
        if let (Some(factory_id), Some(decryption_props)) =
            (&self.factory_id, &self.decryption_properties)
        {
            let factory = Arc::new(StaticEncryptionFactory {
                decryption_properties: Arc::clone(decryption_props),
            }) as Arc<dyn EncryptionFactory>;
            session
                .runtime_env()
                .register_parquet_encryption_factory(factory_id, factory);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SimpleWriterPropertiesFactory — static WriterProperties
// ---------------------------------------------------------------------------

/// A [`WriterPropertiesFactory`] that returns the same static [`WriterProperties`] for
/// every file.
#[derive(Clone, Debug)]
pub struct SimpleWriterPropertiesFactory {
    writer_properties: WriterProperties,
}

impl SimpleWriterPropertiesFactory {
    pub fn new(writer_properties: WriterProperties) -> Self {
        Self { writer_properties }
    }
}

impl Default for SimpleWriterPropertiesFactory {
    fn default() -> Self {
        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_created_by(format!("delta-rs version {}", crate_version()))
            .build();
        Self { writer_properties }
    }
}

#[async_trait]
impl WriterPropertiesFactory for SimpleWriterPropertiesFactory {
    fn compression(&self, column_path: &ColumnPath) -> Compression {
        self.writer_properties.compression(column_path)
    }

    async fn create_writer_properties(
        &self,
        file_path: &Path,
        _file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        debug!("create_writer_properties for file: {file_path}");
        Ok(self.writer_properties.clone())
    }
}

// ---------------------------------------------------------------------------
// StaticEncryptionFactory — returns the same decryption properties for every file
// ---------------------------------------------------------------------------

/// An [`EncryptionFactory`] that returns a fixed set of [`FileDecryptionProperties`] for
/// every file.  Used by [`SimpleFileFormatOptions`] so that static-key encryption goes
/// through the same `factory_id` registration path that KMS uses, avoiding the
/// `TableParquetOptions.crypto.file_decryption` round-trip.
#[derive(Debug)]
struct StaticEncryptionFactory {
    decryption_properties: Arc<FileDecryptionProperties>,
}

#[async_trait]
impl EncryptionFactory for StaticEncryptionFactory {
    async fn get_file_encryption_properties(
        &self,
        _config: &EncryptionFactoryOptions,
        _schema: &Arc<ArrowSchema>,
        _file_path: &Path,
    ) -> datafusion::error::Result<Option<Arc<FileEncryptionProperties>>> {
        Ok(None)
    }

    async fn get_file_decryption_properties(
        &self,
        _config: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> datafusion::error::Result<Option<Arc<FileDecryptionProperties>>> {
        Ok(Some(Arc::clone(&self.decryption_properties)))
    }
}

// ---------------------------------------------------------------------------
// ErrWriterPropertiesFactory — defers a construction error to write time
// ---------------------------------------------------------------------------

/// A [`WriterPropertiesFactory`] that always returns an error from
/// [`create_writer_properties`](WriterPropertiesFactory::create_writer_properties).
///
/// Used by [`SimpleFileFormatOptions::writer_properties_factory`] to defer a
/// `TableParquetOptions`-conversion error to write time (where it can be surfaced as a
/// `DeltaResult`) rather than panicking at construction time.
#[derive(Debug)]
struct ErrWriterPropertiesFactory {
    error: String,
}

#[async_trait]
impl WriterPropertiesFactory for ErrWriterPropertiesFactory {
    fn compression(&self, _column_path: &ColumnPath) -> Compression {
        Compression::UNCOMPRESSED
    }

    async fn create_writer_properties(
        &self,
        _file_path: &Path,
        _file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        Err(DeltaTableError::Generic(self.error.clone()))
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn build_writer_properties_from_tpo(
    table_parquet_options: &Option<TableParquetOptions>,
) -> DeltaResult<Option<WriterProperties>> {
    table_parquet_options
        .as_ref()
        .map(|tpo| {
            let mut tpo = tpo.clone();
            tpo.global.skip_arrow_metadata = true;
            let mut wp_build = WriterPropertiesBuilder::try_from(&tpo).map_err(|e| {
                DeltaTableError::Generic(format!(
                    "Failed to convert TableParquetOptions to WriterProperties: {e}"
                ))
            })?;
            if let Some(enc) = tpo.crypto.file_encryption {
                wp_build = wp_build.with_file_encryption_properties(Arc::new(enc.into()));
            }
            Ok(wp_build.build())
        })
        .transpose()
}

/// Build a default [`WriterPropertiesFactoryRef`] using SNAPPY compression and the
/// delta-rs `created_by` string.
pub fn default_writer_properties_factory() -> WriterPropertiesFactoryRef {
    Arc::new(SimpleWriterPropertiesFactory::default())
}

/// Convenience trait for converting `Option<FileFormatRef>` into a `WriterPropertiesFactoryRef`,
/// using the file-format options' factory if present or the default SNAPPY factory otherwise.
pub trait FileFormatToWriterPropertiesFactory {
    fn into_writer_properties_factory_ref_or_default(self) -> WriterPropertiesFactoryRef;
}

impl FileFormatToWriterPropertiesFactory for Option<FileFormatRef> {
    fn into_writer_properties_factory_ref_or_default(self) -> WriterPropertiesFactoryRef {
        self.map(|ffo| ffo.writer_properties_factory())
            .unwrap_or_else(default_writer_properties_factory)
    }
}
