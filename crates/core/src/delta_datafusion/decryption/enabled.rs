//! Reader decryption for builds with the `encryption` feature.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::config::{EncryptionFactoryOptions, ParquetEncryptionOptions};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use delta_kernel::table_configuration::TableConfiguration;
use object_store::path::Path;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use url::Url;

use crate::errors::DeltaResult;
use crate::operations::write::encryption::resolve_encryption_factory;
use crate::table::config::EncryptionConfig;

/// The KMS factory a scan decrypts its files with, and the crypto options its Parquet
/// sources carry, resolved once per scan from the table's `delta.encryption.*` properties.
#[derive(Debug, Clone, Default)]
pub(crate) struct Decryption {
    factory: Option<Arc<dyn EncryptionFactory>>,
    crypto: ParquetEncryptionOptions,
}

impl Decryption {
    /// Resolve the factory named by the table's `kms_id` from the session's `RuntimeEnv` or
    /// the global registry. An unencrypted table needs no decryption.
    ///
    /// The factory is given file paths relative to `table_root`, the same paths the writer
    /// gave it.
    pub(crate) fn from_table_config(
        config: &TableConfiguration,
        session: &dyn Session,
        table_root: &Url,
    ) -> DeltaResult<Self> {
        let Some(enc) =
            EncryptionConfig::try_from_configuration(config.metadata().configuration())?
        else {
            return Ok(Self::default());
        };
        let runtime_env = session.runtime_env();
        let table_root = Path::from_url_path(table_root.path())?;
        let inner = resolve_encryption_factory(&enc.kms_id, Some(runtime_env))?;
        Ok(Self {
            factory: Some(Arc::new(TableRelativePaths { inner, table_root })),
            crypto: ParquetEncryptionOptions {
                factory_id: Some(enc.kms_id.clone()),
                factory_options: enc.reader_factory_options(),
                ..Default::default()
            },
        })
    }

    /// Give `source` the factory and crypto options, if any, so it can decrypt the files it
    /// reads.
    pub(crate) fn apply(&self, source: ParquetSource) -> ParquetSource {
        let Some(factory) = &self.factory else {
            return source;
        };
        let mut options = source.table_parquet_options().clone();
        options.crypto = self.crypto.clone();
        source
            .with_table_parquet_options(options)
            .with_encryption_factory(Arc::clone(factory))
    }

    /// The decryption keys for `file_path`, if the table is encrypted.
    async fn file_decryption_properties(
        &self,
        file_path: &Path,
    ) -> DeltaResult<Option<Arc<FileDecryptionProperties>>> {
        let Some(factory) = &self.factory else {
            return Ok(None);
        };
        Ok(factory
            .get_file_decryption_properties(&self.crypto.factory_options, file_path)
            .await?)
    }

    /// Add the decryption keys for `file_path` to `options`, for reading a file's metadata
    /// outside a [`ParquetSource`].
    pub(crate) async fn reader_options(
        &self,
        options: ArrowReaderOptions,
        file_path: &Path,
    ) -> DeltaResult<ArrowReaderOptions> {
        Ok(match self.file_decryption_properties(file_path).await? {
            Some(properties) => options.with_file_decryption_properties(properties),
            None => options,
        })
    }

    /// Give `reader` the decryption keys for `file_path`, for fetching a file's footer
    /// outside a [`ParquetSource`].
    pub(crate) async fn metadata_reader<'a>(
        &self,
        reader: DFParquetMetadata<'a>,
        file_path: &Path,
    ) -> DeltaResult<DFParquetMetadata<'a>> {
        Ok(reader.with_decryption_properties(self.file_decryption_properties(file_path).await?))
    }
}

/// Passes `inner` file paths relative to the table root. DataFusion gives a scan's factory
/// the file's location in its object store, which may include the table's own location.
#[derive(Debug)]
struct TableRelativePaths {
    inner: Arc<dyn EncryptionFactory>,
    table_root: Path,
}

impl TableRelativePaths {
    fn relative(&self, file_path: &Path) -> Path {
        match file_path.prefix_match(&self.table_root) {
            Some(parts) => Path::from_iter(parts),
            None => file_path.clone(),
        }
    }
}

#[async_trait]
impl EncryptionFactory for TableRelativePaths {
    async fn get_file_encryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        schema: &SchemaRef,
        file_path: &Path,
    ) -> datafusion::error::Result<Option<Arc<FileEncryptionProperties>>> {
        self.inner
            .get_file_encryption_properties(config, schema, &self.relative(file_path))
            .await
    }

    async fn get_file_decryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> datafusion::error::Result<Option<Arc<FileDecryptionProperties>>> {
        self.inner
            .get_file_decryption_properties(config, &self.relative(file_path))
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Records the paths it is asked to decrypt.
    #[derive(Debug, Default)]
    struct RecordingFactory {
        paths: Mutex<Vec<Path>>,
    }

    #[async_trait]
    impl EncryptionFactory for RecordingFactory {
        async fn get_file_encryption_properties(
            &self,
            _config: &EncryptionFactoryOptions,
            _schema: &SchemaRef,
            _file_path: &Path,
        ) -> datafusion::error::Result<Option<Arc<FileEncryptionProperties>>> {
            Ok(None)
        }

        async fn get_file_decryption_properties(
            &self,
            _config: &EncryptionFactoryOptions,
            file_path: &Path,
        ) -> datafusion::error::Result<Option<Arc<FileDecryptionProperties>>> {
            self.paths.lock().unwrap().push(file_path.clone());
            Ok(None)
        }
    }

    #[tokio::test]
    async fn factory_gets_table_relative_paths() {
        let recording = Arc::new(RecordingFactory::default());
        let factory = TableRelativePaths {
            inner: recording.clone(),
            table_root: Path::from("data/tables/t1"),
        };
        let options = EncryptionFactoryOptions::default();
        for path in [
            "data/tables/t1/_change_data/part-0.parquet",
            "data/tables/t1/year=2024/part-1.parquet",
            "part-2.parquet",
        ] {
            factory
                .get_file_decryption_properties(&options, &Path::from(path))
                .await
                .unwrap();
        }
        assert_eq!(
            *recording.paths.lock().unwrap(),
            [
                Path::from("_change_data/part-0.parquet"),
                Path::from("year=2024/part-1.parquet"),
                Path::from("part-2.parquet"),
            ]
        );
    }

    /// `reader_options` adds the keys needed to read an encrypted file's metadata, as the
    /// change feed does for files with deletion vectors.
    #[tokio::test]
    async fn reader_options_decrypt_file_metadata() {
        use datafusion::prelude::SessionContext;
        use object_store::ObjectStoreExt as _;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        use crate::DeltaTable;
        use crate::logstore::LogStoreExt as _;
        use crate::operations::write::encryption::register_encryption_factory;
        use crate::test_utils::kms_encryption::mock_kms_factory;
        use crate::writer::test_utils::get_record_batch;

        let kms_id = format!("test-kms-{}", uuid::Uuid::new_v4());
        register_encryption_factory(&kms_id, mock_kms_factory());
        let table = DeltaTable::new_in_memory()
            .write(vec![get_record_batch(None, false)])
            .with_configuration([
                ("delta.encryption.kms_id", Some(kms_id.as_str())),
                ("delta.encryption.footer_key", Some("footer-key")),
            ])
            .await
            .unwrap();

        let snapshot = table.snapshot().unwrap();
        let session = SessionContext::new().state();
        let decryption = Decryption::from_table_config(
            snapshot.snapshot().table_configuration(),
            &session,
            &table.log_store().table_root_url(),
        )
        .unwrap();

        let file = snapshot.log_data().into_iter().next().unwrap();
        let path = Path::from(file.path().as_ref());
        let bytes = table
            .object_store()
            .get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let plain = ArrowReaderOptions::new();
        assert!(
            ParquetRecordBatchReaderBuilder::try_new_with_options(bytes.clone(), plain.clone())
                .is_err(),
            "the footer is encrypted"
        );
        let decrypting = decryption.reader_options(plain, &path).await.unwrap();
        ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, decrypting).unwrap();
    }
}
