//! Reader decryption for builds without the `encryption` feature: there is nothing to
//! decrypt with, and the protocol checker refuses encrypted tables before a scan is planned.

use datafusion::catalog::Session;
use datafusion::config::TableParquetOptions;
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use delta_kernel::table_configuration::TableConfiguration;
use object_store::path::Path;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use url::Url;

use crate::errors::DeltaResult;

/// No decryption options can be derived without the `encryption` feature.
pub(crate) fn parquet_options_from_table_config(
    _config: &TableConfiguration,
) -> DeltaResult<Option<TableParquetOptions>> {
    Ok(None)
}

/// Stand-in for the scan's decryption factory; leaves sources and options unchanged.
#[derive(Debug, Clone, Default)]
pub(crate) struct Decryption {}

impl Decryption {
    pub(crate) fn try_new(
        _options: &TableParquetOptions,
        _session: &dyn Session,
        _table_root: &Url,
    ) -> DeltaResult<Self> {
        Ok(Self {})
    }

    pub(crate) fn apply(&self, source: ParquetSource) -> ParquetSource {
        source
    }

    pub(crate) async fn reader_options(
        &self,
        options: ArrowReaderOptions,
        _file_path: &Path,
    ) -> DeltaResult<ArrowReaderOptions> {
        Ok(options)
    }

    pub(crate) async fn metadata_reader<'a>(
        &self,
        reader: DFParquetMetadata<'a>,
        _file_path: &Path,
    ) -> DeltaResult<DFParquetMetadata<'a>> {
        Ok(reader)
    }
}
