//! Reader decryption for builds with the `encryption` feature.

use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use delta_kernel::table_configuration::TableConfiguration;

use crate::errors::DeltaResult;
use crate::operations::write::encryption::resolve_encryption_factory_or_err;
use crate::table::config::EncryptionConfig;

/// Derive [`TableParquetOptions`] from `delta.encryption.*` table properties, or `None`
/// for an unencrypted table.
pub(crate) fn parquet_options_from_table_config(
    config: &TableConfiguration,
) -> DeltaResult<Option<TableParquetOptions>> {
    Ok(
        EncryptionConfig::try_from_properties(config.table_properties())?
            .map(|enc| enc.to_table_parquet_options()),
    )
}

/// The KMS factory a scan decrypts its files with, resolved once per scan.
#[derive(Debug, Clone)]
pub(crate) struct Decryption {
    factory: Option<Arc<dyn EncryptionFactory>>,
}

impl Decryption {
    /// Resolve the factory named in `options` from the session's `RuntimeEnv` or the global
    /// registry. Options without a factory (an unencrypted table) need no decryption.
    pub(crate) fn try_new(
        options: &TableParquetOptions,
        session: &dyn Session,
    ) -> DeltaResult<Self> {
        let factory = options
            .crypto
            .factory_id
            .as_deref()
            .map(|id| resolve_encryption_factory_or_err(id, session))
            .transpose()?;
        Ok(Self { factory })
    }

    /// Attach the factory, if any, to `source` so it can decrypt the files it reads.
    pub(crate) fn apply(&self, source: ParquetSource) -> ParquetSource {
        match &self.factory {
            Some(factory) => source.with_encryption_factory(Arc::clone(factory)),
            None => source,
        }
    }
}
