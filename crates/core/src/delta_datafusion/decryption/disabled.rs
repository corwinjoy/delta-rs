//! Reader decryption for builds without the `encryption` feature: there is nothing to
//! decrypt with, and the protocol checker refuses encrypted tables before a scan is planned.

use datafusion::catalog::Session;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::physical_plan::ParquetSource;
use delta_kernel::table_configuration::TableConfiguration;

use crate::errors::DeltaResult;

/// No decryption options can be derived without the `encryption` feature.
pub(crate) fn parquet_options_from_table_config(
    _config: &TableConfiguration,
) -> DeltaResult<Option<TableParquetOptions>> {
    Ok(None)
}

/// Stand-in for the scan's decryption factory; leaves sources unchanged.
#[derive(Debug, Clone)]
pub(crate) struct Decryption;

impl Decryption {
    pub(crate) fn try_new(
        _options: &TableParquetOptions,
        _session: &dyn Session,
    ) -> DeltaResult<Self> {
        Ok(Self)
    }

    pub(crate) fn apply(&self, source: ParquetSource) -> ParquetSource {
        source
    }
}
