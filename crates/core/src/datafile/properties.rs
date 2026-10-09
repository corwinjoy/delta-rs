//! Engine-agnostic Delta read and write configuration.
//!
//! [`DeltaWriterProperties`] is everything delta-rs needs to encode data files:
//! the parquet [`WriterProperties`], the [`ArrowWriterOptions`], file and batch
//! sizes, and the data-skipping statistics to collect. Every write path carries
//! one value of it end to end instead of its own subset of these knobs.
//!
//! [`ReaderProperties`] centralizes construction of DataFusion's
//! [`TableParquetOptions`](datafusion::config::TableParquetOptions) for Delta
//! scans, so read/parquet-IO config (future: per-file decryption) lives in one
//! place. Read-side counterpart to [`DeltaWriterProperties`].

use std::num::NonZeroU64;

use delta_kernel::table_configuration::TableConfiguration;
use delta_kernel::table_properties::DataSkippingNumIndexedCols;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::datafile::writer::ArrowWriterOptions;
use crate::kernel::arrow::engine_ext::stats_table_properties;
use crate::parquet_utils::default_writer_properties;
use crate::table::config::{DEFAULT_NUM_INDEX_COLS, TablePropertiesExt as _};

/// Rows handed to the parquet writer per slice when no other size is set.
pub const DEFAULT_WRITE_BATCH_SIZE: usize = 8192;

/// Engine-agnostic parquet read configuration for a Delta scan.
// Future fields (e.g. per-file decryption) attach here.
#[derive(Clone, Debug, Default)]
pub struct ReaderProperties {}

#[cfg(feature = "datafusion")]
impl ReaderProperties {
    /// Build DataFusion's `TableParquetOptions` for a `ParquetSource`, inheriting
    /// the session's parquet execution settings.
    pub fn to_table_parquet_options(
        &self,
        session: &dyn datafusion::catalog::Session,
    ) -> datafusion::config::TableParquetOptions {
        datafusion::config::TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        }
    }
}

/// Configuration for the writer on how to collect stats
#[derive(Clone, Debug)]
pub struct WriterStatsConfig {
    /// Number of columns to collect stats for, idx based
    pub num_indexed_cols: DataSkippingNumIndexedCols,
    /// Optional list of columns which to collect stats for, takes precedende over num_index_cols
    pub stats_columns: Option<Vec<String>>,
}

impl Default for WriterStatsConfig {
    fn default() -> Self {
        Self {
            num_indexed_cols: DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS),
            stats_columns: None,
        }
    }
}

impl WriterStatsConfig {
    /// Create new writer stats config
    pub fn new(
        num_indexed_cols: DataSkippingNumIndexedCols,
        stats_columns: Option<Vec<String>>,
    ) -> Self {
        Self {
            num_indexed_cols,
            stats_columns,
        }
    }

    /// Derive writer statistics configuration from a table's [`TableConfiguration`].
    pub fn from_config(config: &TableConfiguration) -> Self {
        let properties = stats_table_properties(
            config.logical_schema().as_ref(),
            config.table_properties(),
            config.column_mapping_mode(),
        );
        Self {
            num_indexed_cols: properties.num_indexed_cols(),
            stats_columns: properties
                .data_skipping_stats_columns
                .as_ref()
                .map(|columns| columns.iter().map(|c| c.to_string()).collect()),
        }
    }
}

/// Everything delta-rs needs to encode Delta data files.
///
/// Wraps the parquet [`WriterProperties`] together with the delta-rs specific
/// knobs that every write path used to carry separately. Unset fields fall back
/// to delta-rs defaults, so `Default::default()` is a complete configuration.
#[derive(Clone, Debug, Default)]
pub struct DeltaWriterProperties {
    /// Parquet writer properties. `None` means the delta-rs default (SNAPPY,
    /// delta-rs `created_by`), or whatever default the operation chooses.
    parquet: Option<WriterProperties>,
    /// Options for the arrow writer on top of parquet.
    arrow: ArrowWriterOptions,
    /// Size above which a data file is closed and a new one started.
    /// `None` means a single file per partition until the writer is closed.
    target_file_size: Option<NonZeroU64>,
    /// Rows per slice handed to the parquet writer. With the writer's row-group
    /// settings this bounds how precisely file sizes are tracked.
    write_batch_size: Option<usize>,
    /// Which columns to collect Delta data-skipping statistics for.
    stats: WriterStatsConfig,
}

impl DeltaWriterProperties {
    /// Use these parquet writer properties instead of the delta-rs default.
    pub fn with_parquet_properties(mut self, properties: WriterProperties) -> Self {
        self.parquet = Some(properties);
        self
    }

    /// Use these arrow writer options.
    pub fn with_arrow_options(mut self, options: ArrowWriterOptions) -> Self {
        self.arrow = options;
        self
    }

    /// Close a data file once it reaches `size`; `None` never rolls.
    pub fn with_target_file_size(mut self, size: Option<NonZeroU64>) -> Self {
        self.target_file_size = size;
        self
    }

    /// Rows per slice handed to the parquet writer. Zero is rejected when a
    /// writer is built from these properties.
    pub fn with_write_batch_size(mut self, rows: usize) -> Self {
        self.write_batch_size = Some(rows);
        self
    }

    /// Which columns to collect Delta data-skipping statistics for.
    pub fn with_stats_config(mut self, stats: WriterStatsConfig) -> Self {
        self.stats = stats;
        self
    }

    /// The parquet writer properties set on these, if any.
    pub fn parquet_properties(&self) -> Option<&WriterProperties> {
        self.parquet.as_ref()
    }

    /// The arrow writer options.
    pub fn arrow_options(&self) -> &ArrowWriterOptions {
        &self.arrow
    }

    /// The size above which data files roll, if any.
    pub fn target_file_size(&self) -> Option<NonZeroU64> {
        self.target_file_size
    }

    /// Rows per slice handed to the parquet writer, if set.
    pub fn write_batch_size(&self) -> Option<usize> {
        self.write_batch_size
    }

    /// Which columns to collect Delta data-skipping statistics for.
    pub fn stats(&self) -> &WriterStatsConfig {
        &self.stats
    }

    /// The parquet writer properties every file starts from: the ones set here,
    /// or the delta-rs default (SNAPPY, delta-rs `created_by`).
    pub fn base_parquet_properties(&self) -> WriterProperties {
        self.parquet
            .clone()
            .unwrap_or_else(|| default_writer_properties(Compression::SNAPPY))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use delta_kernel::schema::{DataType, StructField, StructType};
    use parquet::schema::types::ColumnPath;

    use super::*;
    use crate::test_utils::{build_test_table_configuration, column_mapping_test_field};

    #[test]
    fn defaults_fall_back_to_delta_rs_parquet_properties() {
        let props = DeltaWriterProperties::default();
        assert!(props.parquet_properties().is_none());
        let base = props.base_parquet_properties();
        assert_eq!(
            base.created_by(),
            format!("delta-rs version {}", crate::crate_version())
        );
        assert_eq!(
            base.compression(&ColumnPath::from("id")),
            Compression::SNAPPY
        );
        assert!(props.target_file_size().is_none());
        assert!(props.write_batch_size().is_none());
        assert_eq!(
            props.stats().num_indexed_cols,
            DataSkippingNumIndexedCols::NumColumns(DEFAULT_NUM_INDEX_COLS)
        );
    }

    #[test]
    fn setters_round_trip() {
        let parquet = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build();
        let props = DeltaWriterProperties::default()
            .with_parquet_properties(parquet)
            .with_arrow_options(ArrowWriterOptions::new().with_enable_parallel_encoding(false))
            .with_target_file_size(NonZeroU64::new(10))
            .with_write_batch_size(7)
            .with_stats_config(WriterStatsConfig::new(
                DataSkippingNumIndexedCols::AllColumns,
                Some(vec!["a".to_string()]),
            ));
        assert_eq!(
            props
                .base_parquet_properties()
                .compression(&ColumnPath::from("id")),
            Compression::UNCOMPRESSED
        );
        assert!(!props.arrow_options().enable_parallel_encoding());
        assert_eq!(props.target_file_size(), NonZeroU64::new(10));
        assert_eq!(props.write_batch_size(), Some(7));
        assert_eq!(props.stats().stats_columns, Some(vec!["a".to_string()]));
    }

    #[test]
    fn from_config_translates_stats_columns_to_physical_names() {
        // `physical_name` resolves to the `physicalName` annotation under both name and id modes.
        for mode in ["name", "id"] {
            let logical_schema = StructType::try_new([
                column_mapping_test_field("p", "col_p", 1),
                column_mapping_test_field("a", "col_a", 2),
            ])
            .unwrap();
            let table_config = build_test_table_configuration(
                logical_schema,
                vec!["p".to_string()],
                HashMap::from([
                    ("delta.columnMapping.mode".to_string(), mode.to_string()),
                    (
                        "delta.dataSkippingStatsColumns".to_string(),
                        "a".to_string(),
                    ),
                ]),
            );

            let config = WriterStatsConfig::from_config(&table_config);
            assert_eq!(
                config.stats_columns,
                Some(vec!["col_a".to_string()]),
                "stats columns should be physical names in {mode} mode"
            );
        }
    }

    #[test]
    fn from_config_keeps_logical_names_without_column_mapping() {
        let logical_schema = StructType::try_new([
            StructField::nullable("a", DataType::STRING),
            StructField::nullable("b", DataType::STRING),
        ])
        .unwrap();
        let table_config = build_test_table_configuration(
            logical_schema,
            vec![],
            HashMap::from([(
                "delta.dataSkippingStatsColumns".to_string(),
                "a".to_string(),
            )]),
        );

        let config = WriterStatsConfig::from_config(&table_config);
        assert_eq!(config.stats_columns, Some(vec!["a".to_string()]));
    }
}
