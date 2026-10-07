//! Delta Table configuration
use std::collections::HashMap;
use std::num::{NonZero, NonZeroU64};
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

#[cfg(all(feature = "datafusion", feature = "encryption"))]
use datafusion::config::{EncryptionFactoryOptions, TableParquetOptions};
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{DataType, StructType};
use delta_kernel::table_features::ColumnMappingMode;
use delta_kernel::table_properties::{DataSkippingNumIndexedCols, IsolationLevel, TableProperties};

use super::Constraint;
use crate::errors::{DeltaResult, DeltaTableError};

/// Typed property keys that can be defined on a delta table
///
/// <https://docs.delta.io/latest/table-properties.html#delta-table-properties-reference>
/// <https://learn.microsoft.com/en-us/azure/databricks/delta/table-properties>
#[derive(PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum TableProperty {
    /// true for this Delta table to be append-only. If append-only,
    /// existing records cannot be deleted, and existing values cannot be updated.
    AppendOnly,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table.
    AutoOptimizeAutoCompact,

    /// true for Delta Lake to automatically optimize the layout of the files for this Delta table during writes.
    AutoOptimizeOptimizeWrite,

    /// Interval (number of commits) after which a new checkpoint should be created
    CheckpointInterval,

    /// true for Delta Lake to write file statistics in checkpoints in JSON format for the stats column.
    CheckpointWriteStatsAsJson,

    /// true for Delta Lake to write file statistics to checkpoints in struct format for the
    /// stats_parsed column and to write partition values as a struct for partitionValues_parsed.
    CheckpointWriteStatsAsStruct,

    /// true for Delta Lake to write checkpoint files using run length encoding (RLE).
    /// Some readers don't support run length encoding (i.e. Fabric) so this can be disabled.
    CheckpointUseRunLengthEncoding,

    /// Whether column mapping is enabled for Delta table columns and the corresponding
    /// Parquet columns that use different names.
    ColumnMappingMode,

    /// The number of columns for Delta Lake to collect statistics about for data skipping.
    /// A value of -1 means to collect statistics for all columns. Updating this property does
    /// not automatically collect statistics again; instead, it redefines the statistics schema
    /// of the Delta table. Specifically, it changes the behavior of future statistics collection
    /// (such as during appends and optimizations) as well as data skipping (such as ignoring column
    /// statistics beyond this number, even when such statistics exist).
    DataSkippingNumIndexedCols,

    /// A comma-separated list of column names on which Delta Lake collects statistics to enhance
    /// data skipping functionality. This property takes precedence over
    /// [DataSkippingNumIndexedCols](Self::DataSkippingNumIndexedCols).
    DataSkippingStatsColumns,

    /// The shortest duration for Delta Lake to keep logically deleted data files before deleting
    /// them physically. This is to prevent failures in stale readers after compactions or partition overwrites.
    ///
    /// This value should be large enough to ensure that:
    ///
    /// * It is larger than the longest possible duration of a job if you run VACUUM when there are
    ///   concurrent readers or writers accessing the Delta table.
    /// * If you run a streaming query that reads from the table, that query does not stop for longer
    ///   than this value. Otherwise, the query may not be able to restart, as it must still read old files.
    DeletedFileRetentionDuration,

    /// true to enable change data feed.
    EnableChangeDataFeed,

    /// true to enable deletion vectors and predictive I/O for updates.
    EnableDeletionVectors,

    /// The degree to which a transaction must be isolated from modifications made by concurrent transactions.
    ///
    /// Valid values are `Serializable` and `WriteSerializable`.
    IsolationLevel,

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log size increases.
    LogRetentionDuration,

    /// TODO I could not find this property in the documentation, but was defined here and makes sense..?
    EnableExpiredLogCleanup,

    /// The minimum required protocol reader version for a reader that allows to read from this Delta table.
    MinReaderVersion,

    /// The minimum required protocol writer version for a writer that allows to write to this Delta table.
    MinWriterVersion,

    /// true for Delta Lake to generate a random prefix for a file path instead of partition information.
    ///
    /// For example, this ma
    /// y improve Amazon S3 performance when Delta Lake needs to send very high volumes
    /// of Amazon S3 calls to better partition across S3 servers.
    RandomizeFilePrefixes,

    /// When delta.randomizeFilePrefixes is set to true, the number of characters that Delta Lake generates for random prefixes.
    RandomPrefixLength,

    /// The shortest duration within which new snapshots will retain transaction identifiers (for example, SetTransactions).
    /// When a new snapshot sees a transaction identifier older than or equal to the duration specified by this property,
    /// the snapshot considers it expired and ignores it. The SetTransaction identifier is used when making the writes idempotent.
    SetTransactionRetentionDuration,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.
    TargetFileSize,

    /// The target file size in bytes or higher units for file tuning. For example, 104857600 (bytes) or 100mb.
    TuneFileSizesForRewrites,

    /// 'classic' for classic Delta Lake checkpoints. 'v2' for v2 checkpoints.
    CheckpointPolicy,

    /// The KMS client used to encrypt the table's Parquet files. See [`EncryptionConfig`].
    EncryptionKmsId,

    /// Opaque, KMS-specific configuration for the encryption KMS client. See [`EncryptionConfig`].
    EncryptionKmsConfiguration,

    /// The master key ID used for Parquet footer encryption; setting it enables encryption.
    /// See [`EncryptionConfig`].
    EncryptionFooterKey,

    /// true to leave Parquet footers unencrypted. See [`EncryptionConfig`].
    EncryptionPlaintextFooter,

    /// Master key IDs mapped to the columns they encrypt, as `keyId:col1,col2;keyId2:col3`.
    /// See [`EncryptionConfig`].
    EncryptionColumnKeys,
}

impl AsRef<str> for TableProperty {
    fn as_ref(&self) -> &str {
        match self {
            Self::AppendOnly => "delta.appendOnly",
            Self::CheckpointInterval => "delta.checkpointInterval",
            Self::AutoOptimizeAutoCompact => "delta.autoOptimize.autoCompact",
            Self::AutoOptimizeOptimizeWrite => "delta.autoOptimize.optimizeWrite",
            Self::CheckpointWriteStatsAsJson => "delta.checkpoint.writeStatsAsJson",
            Self::CheckpointWriteStatsAsStruct => "delta.checkpoint.writeStatsAsStruct",
            Self::CheckpointUseRunLengthEncoding => "delta-rs.checkpoint.useRunLengthEncoding",
            Self::CheckpointPolicy => "delta.checkpointPolicy",
            Self::ColumnMappingMode => "delta.columnMapping.mode",
            Self::DataSkippingNumIndexedCols => "delta.dataSkippingNumIndexedCols",
            Self::DataSkippingStatsColumns => "delta.dataSkippingStatsColumns",
            Self::DeletedFileRetentionDuration => "delta.deletedFileRetentionDuration",
            Self::EnableChangeDataFeed => "delta.enableChangeDataFeed",
            Self::EnableDeletionVectors => "delta.enableDeletionVectors",
            Self::IsolationLevel => "delta.isolationLevel",
            Self::LogRetentionDuration => "delta.logRetentionDuration",
            Self::EnableExpiredLogCleanup => "delta.enableExpiredLogCleanup",
            Self::MinReaderVersion => "delta.minReaderVersion",
            Self::MinWriterVersion => "delta.minWriterVersion",
            Self::RandomizeFilePrefixes => "delta.randomizeFilePrefixes",
            Self::RandomPrefixLength => "delta.randomPrefixLength",
            Self::SetTransactionRetentionDuration => "delta.setTransactionRetentionDuration",
            Self::TargetFileSize => "delta.targetFileSize",
            Self::TuneFileSizesForRewrites => "delta.tuneFileSizesForRewrites",
            Self::EncryptionKmsId => ENCRYPTION_KMS_ID_PROP,
            Self::EncryptionKmsConfiguration => ENCRYPTION_KMS_CONFIGURATION_PROP,
            Self::EncryptionFooterKey => ENCRYPTION_FOOTER_KEY_PROP,
            Self::EncryptionPlaintextFooter => ENCRYPTION_PLAINTEXT_FOOTER_PROP,
            Self::EncryptionColumnKeys => ENCRYPTION_COLUMN_KEYS_PROP,
        }
    }
}

impl FromStr for TableProperty {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "delta.appendOnly" => Ok(Self::AppendOnly),
            "delta.checkpointInterval" => Ok(Self::CheckpointInterval),
            "delta.autoOptimize.autoCompact" => Ok(Self::AutoOptimizeAutoCompact),
            "delta.autoOptimize.optimizeWrite" => Ok(Self::AutoOptimizeOptimizeWrite),
            "delta.checkpoint.writeStatsAsJson" => Ok(Self::CheckpointWriteStatsAsJson),
            "delta.checkpoint.writeStatsAsStruct" => Ok(Self::CheckpointWriteStatsAsStruct),
            "delta-rs.checkpoint.useRunLengthEncoding" => Ok(Self::CheckpointUseRunLengthEncoding),
            "delta.checkpointPolicy" => Ok(Self::CheckpointPolicy),
            "delta.columnMapping.mode" => Ok(Self::ColumnMappingMode),
            "delta.dataSkippingNumIndexedCols" => Ok(Self::DataSkippingNumIndexedCols),
            "delta.dataSkippingStatsColumns" => Ok(Self::DataSkippingStatsColumns),
            "delta.deletedFileRetentionDuration" | "deletedFileRetentionDuration" => {
                Ok(Self::DeletedFileRetentionDuration)
            }
            "delta.enableChangeDataFeed" => Ok(Self::EnableChangeDataFeed),
            "delta.enableDeletionVectors" => Ok(Self::EnableDeletionVectors),
            "delta.isolationLevel" => Ok(Self::IsolationLevel),
            "delta.logRetentionDuration" | "logRetentionDuration" => Ok(Self::LogRetentionDuration),
            "delta.enableExpiredLogCleanup" | "enableExpiredLogCleanup" => {
                Ok(Self::EnableExpiredLogCleanup)
            }
            "delta.minReaderVersion" => Ok(Self::MinReaderVersion),
            "delta.minWriterVersion" => Ok(Self::MinWriterVersion),
            "delta.randomizeFilePrefixes" => Ok(Self::RandomizeFilePrefixes),
            "delta.randomPrefixLength" => Ok(Self::RandomPrefixLength),
            "delta.setTransactionRetentionDuration" => Ok(Self::SetTransactionRetentionDuration),
            "delta.targetFileSize" => Ok(Self::TargetFileSize),
            "delta.tuneFileSizesForRewrites" => Ok(Self::TuneFileSizesForRewrites),
            ENCRYPTION_KMS_ID_PROP => Ok(Self::EncryptionKmsId),
            ENCRYPTION_KMS_CONFIGURATION_PROP => Ok(Self::EncryptionKmsConfiguration),
            ENCRYPTION_FOOTER_KEY_PROP => Ok(Self::EncryptionFooterKey),
            ENCRYPTION_PLAINTEXT_FOOTER_PROP => Ok(Self::EncryptionPlaintextFooter),
            ENCRYPTION_COLUMN_KEYS_PROP => Ok(Self::EncryptionColumnKeys),
            _ => Err(DeltaTableError::Generic("unknown config key".into())),
        }
    }
}

/// Delta configuration error
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum DeltaConfigError {
    /// Error returned when configuration validation failed.
    #[error("Validation failed - {0}")]
    Validation(String),
}

/// Default num index cols
pub const DEFAULT_NUM_INDEX_COLS: u64 = 32;
/// Default target file size
pub const DEFAULT_TARGET_FILE_SIZE: NonZeroU64 = NonZeroU64::new(100 * 1024 * 1024).unwrap();

/// Convenience accessors for reading well-known Delta table properties with their defaults
/// applied, layered on top of the raw [`TableProperties`] parsed from table metadata.
pub trait TablePropertiesExt {
    /// true for this Delta table to be append-only. If append-only, existing records cannot be
    /// deleted, and existing values cannot be updated. See [append-only tables] in the protocol.
    ///
    /// [append-only tables]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#append-only-tables
    fn append_only(&self) -> bool;

    /// How long the history for a Delta table is kept.
    ///
    /// Each time a checkpoint is written, Delta Lake automatically cleans up log entries older
    /// than the retention interval. If you set this property to a large enough value, many log
    /// entries are retained. This should not impact performance as operations against the log are
    /// constant time. Operations on history are parallel but will become more expensive as the log
    /// size increases.
    fn log_retention_duration(&self) -> Duration;

    /// Whether to clean up expired checkpoints/commits in the delta log.
    fn enable_expired_log_cleanup(&self) -> bool;

    /// Interval (expressed as number of commits) after which a new checkpoint should be created.
    /// E.g. if checkpoint interval = 10, then a checkpoint should be written every 10 commits.
    fn checkpoint_interval(&self) -> NonZero<u64>;

    /// Number of columns to be indexed.
    fn num_indexed_cols(&self) -> DataSkippingNumIndexedCols;

    /// Target size in bytes for data files produced by writes and compaction.
    fn target_file_size(&self) -> NonZero<u64>;

    /// Whether the Change Data Feed is enabled for this table.
    fn enable_change_data_feed(&self) -> bool;

    /// How long removed data files are retained before they may be physically deleted by vacuum.
    fn deleted_file_retention_duration(&self) -> Duration;

    /// The isolation level used when checking for conflicts during commits.
    fn isolation_level(&self) -> IsolationLevel;

    /// The list of constraints (e.g. CHECK constraints) declared on the table.
    fn get_constraints(&self) -> Vec<Constraint>;
}

impl TablePropertiesExt for TableProperties {
    fn append_only(&self) -> bool {
        self.append_only.unwrap_or(false)
    }

    fn log_retention_duration(&self) -> Duration {
        static DEFAULT_DURATION: LazyLock<Duration> =
            LazyLock::new(|| parse_interval("interval 30 days").unwrap());
        self.log_retention_duration
            .unwrap_or(DEFAULT_DURATION.to_owned())
    }

    fn enable_expired_log_cleanup(&self) -> bool {
        self.enable_expired_log_cleanup.unwrap_or(true)
    }

    fn checkpoint_interval(&self) -> NonZero<u64> {
        static DEFAULT_INTERVAL: LazyLock<NonZero<u64>> =
            LazyLock::new(|| NonZero::new(100).unwrap());
        self.checkpoint_interval
            .unwrap_or(DEFAULT_INTERVAL.to_owned())
    }

    fn num_indexed_cols(&self) -> DataSkippingNumIndexedCols {
        self.data_skipping_num_indexed_cols
            .unwrap_or(DataSkippingNumIndexedCols::NumColumns(32))
    }

    fn target_file_size(&self) -> NonZeroU64 {
        self.target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE)
    }

    fn enable_change_data_feed(&self) -> bool {
        self.enable_change_data_feed.unwrap_or(false)
    }

    fn deleted_file_retention_duration(&self) -> Duration {
        static DEFAULT_DURATION: LazyLock<Duration> =
            LazyLock::new(|| parse_interval("interval 1 weeks").unwrap());
        self.deleted_file_retention_duration
            .unwrap_or(DEFAULT_DURATION.to_owned())
    }

    fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level.unwrap_or_default()
    }

    /// Return the check constraints on the current table
    fn get_constraints(&self) -> Vec<Constraint> {
        // TODO: upstream parsing of constraints to delta-kernel
        self.unknown_properties
            .iter()
            .filter_map(|(field, value)| {
                if field.starts_with("delta.constraints") {
                    let constraint_name = field.replace("delta.constraints.", "");
                    Some(Constraint::new(&constraint_name, value))
                } else {
                    None
                }
            })
            .collect()
    }
}

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

fn parse_interval(value: &str) -> Result<Duration, DeltaConfigError> {
    let not_an_interval = || DeltaConfigError::Validation(format!("'{value}' is not an interval"));

    if !value.starts_with("interval ") {
        return Err(not_an_interval());
    }
    let mut it = value.split_whitespace();
    let _ = it.next(); // skip "interval"
    let number = parse_int(it.next().ok_or_else(not_an_interval)?)?;
    if number < 0 {
        return Err(DeltaConfigError::Validation(format!(
            "interval '{value}' cannot be negative"
        )));
    }
    let number = number as u64;

    let duration = match it.next().ok_or_else(not_an_interval)? {
        "nanosecond" | "nanoseconds" => Duration::from_nanos(number),
        "microsecond" | "microseconds" => Duration::from_micros(number),
        "millisecond" | "milliseconds" => Duration::from_millis(number),
        "second" | "seconds" => Duration::from_secs(number),
        "minute" | "minutes" => Duration::from_secs(number * SECONDS_PER_MINUTE),
        "hour" | "hours" => Duration::from_secs(number * SECONDS_PER_HOUR),
        "day" | "days" => Duration::from_secs(number * SECONDS_PER_DAY),
        "week" | "weeks" => Duration::from_secs(number * SECONDS_PER_WEEK),
        unit => {
            return Err(DeltaConfigError::Validation(format!(
                "Unknown unit '{unit}'"
            )));
        }
    };

    Ok(duration)
}

fn parse_int(value: &str) -> Result<i64, DeltaConfigError> {
    value.parse().map_err(|e| {
        DeltaConfigError::Validation(format!("Cannot parse '{value}' as integer: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_interval_test() {
        assert_eq!(
            parse_interval("interval 123 nanosecond").unwrap(),
            Duration::from_nanos(123)
        );

        assert_eq!(
            parse_interval("interval 123 nanoseconds").unwrap(),
            Duration::from_nanos(123)
        );

        assert_eq!(
            parse_interval("interval 123 microsecond").unwrap(),
            Duration::from_micros(123)
        );

        assert_eq!(
            parse_interval("interval 123 microseconds").unwrap(),
            Duration::from_micros(123)
        );

        assert_eq!(
            parse_interval("interval 123 millisecond").unwrap(),
            Duration::from_millis(123)
        );

        assert_eq!(
            parse_interval("interval 123 milliseconds").unwrap(),
            Duration::from_millis(123)
        );

        assert_eq!(
            parse_interval("interval 123 second").unwrap(),
            Duration::from_secs(123)
        );

        assert_eq!(
            parse_interval("interval 123 seconds").unwrap(),
            Duration::from_secs(123)
        );

        assert_eq!(
            parse_interval("interval 123 minute").unwrap(),
            Duration::from_secs(123 * 60)
        );

        assert_eq!(
            parse_interval("interval 123 minutes").unwrap(),
            Duration::from_secs(123 * 60)
        );

        assert_eq!(
            parse_interval("interval 123 hour").unwrap(),
            Duration::from_secs(123 * 3600)
        );

        assert_eq!(
            parse_interval("interval 123 hours").unwrap(),
            Duration::from_secs(123 * 3600)
        );

        assert_eq!(
            parse_interval("interval 123 day").unwrap(),
            Duration::from_secs(123 * 86400)
        );

        assert_eq!(
            parse_interval("interval 123 days").unwrap(),
            Duration::from_secs(123 * 86400)
        );

        assert_eq!(
            parse_interval("interval 123 week").unwrap(),
            Duration::from_secs(123 * 604800)
        );

        assert_eq!(
            parse_interval("interval 123 week").unwrap(),
            Duration::from_secs(123 * 604800)
        );
    }

    #[test]
    fn parse_interval_invalid_test() {
        assert_eq!(
            parse_interval("whatever").err().unwrap(),
            DeltaConfigError::Validation("'whatever' is not an interval".to_string())
        );

        assert_eq!(
            parse_interval("interval").err().unwrap(),
            DeltaConfigError::Validation("'interval' is not an interval".to_string())
        );

        assert_eq!(
            parse_interval("interval 2").err().unwrap(),
            DeltaConfigError::Validation("'interval 2' is not an interval".to_string())
        );

        assert_eq!(
            parse_interval("interval 2 years").err().unwrap(),
            DeltaConfigError::Validation("Unknown unit 'years'".to_string())
        );

        assert_eq!(
            parse_interval("interval two years").err().unwrap(),
            DeltaConfigError::Validation(
                "Cannot parse 'two' as integer: invalid digit found in string".to_string()
            )
        );

        assert_eq!(
            parse_interval("interval -25 hours").err().unwrap(),
            DeltaConfigError::Validation(
                "interval 'interval -25 hours' cannot be negative".to_string()
            )
        );
    }
}

// ---------------------------------------------------------------------------
// EncryptionConfig — parsed from delta.encryption.* table properties
// ---------------------------------------------------------------------------
//
// Names and semantics follow the protocol RFC: https://github.com/delta-io/delta/issues/6195
//
// The properties are read from the raw `configuration` map of the table's metadata, never
// from delta-kernel's `TableProperties`. Kernel keeps only the keys it does not recognise in
// `TableProperties::unknown_properties`, so the day it learns these keys they would vanish
// from there and an encrypted table would look like a plaintext one to the writer. Reading
// the metadata map directly keeps that from ever happening.

/// Prefix shared by all encryption table properties.
pub const ENCRYPTION_PROP_PREFIX: &str = "delta.encryption.";
/// Table property naming the KMS client used to encrypt the table.
pub const ENCRYPTION_KMS_ID_PROP: &str = "delta.encryption.kms_id";
/// Table property holding opaque, KMS-specific configuration.
pub const ENCRYPTION_KMS_CONFIGURATION_PROP: &str = "delta.encryption.kms_configuration";
/// Table property holding the master key ID used for footer encryption.
pub const ENCRYPTION_FOOTER_KEY_PROP: &str = "delta.encryption.footer_key";
/// Table property controlling whether Parquet footers are left unencrypted.
pub const ENCRYPTION_PLAINTEXT_FOOTER_PROP: &str = "delta.encryption.plaintext_footer";
/// Table property mapping master key IDs to columns, as `keyId:col1,col2;keyId2:col3`.
pub const ENCRYPTION_COLUMN_KEYS_PROP: &str = "delta.encryption.column_keys";

const ENCRYPTION_PROPS: [&str; 5] = [
    ENCRYPTION_KMS_ID_PROP,
    ENCRYPTION_KMS_CONFIGURATION_PROP,
    ENCRYPTION_FOOTER_KEY_PROP,
    ENCRYPTION_PLAINTEXT_FOOTER_PROP,
    ENCRYPTION_COLUMN_KEYS_PROP,
];

/// Whether two dot-separated column paths are the same or one contains the other.
fn column_paths_overlap(a: &str, b: &str) -> bool {
    let (shorter, longer) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    longer
        .strip_prefix(shorter)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
}

fn invalid_encryption_config(msg: String) -> DeltaTableError {
    DeltaTableError::Generic(format!("Invalid table encryption configuration: {msg}"))
}

/// The physical, dot-separated path of the display-name path `column`, or `None` if it is
/// not in `schema`. Without column mapping the two are the same.
fn physical_path(
    schema: &StructType,
    column_mapping_mode: ColumnMappingMode,
    column: &str,
) -> Option<String> {
    let mut fields = Some(schema);
    let mut physical = Vec::new();
    for part in column.split('.') {
        let field = fields?.field(part)?;
        physical.push(field.physical_name(column_mapping_mode).to_string());
        fields = match field.data_type() {
            DataType::Struct(inner) => Some(inner.as_ref()),
            _ => None,
        };
    }
    Some(physical.join("."))
}

/// Option keys passed to the [`EncryptionFactoryOptions`]: the property names without the
/// `delta.encryption.` prefix.
#[cfg(all(feature = "datafusion", feature = "encryption"))]
pub(crate) const FACTORY_OPT_KMS_CONFIGURATION: &str = "kms_configuration";
#[cfg(all(feature = "datafusion", feature = "encryption"))]
pub(crate) const FACTORY_OPT_FOOTER_KEY: &str = "footer_key";
#[cfg(all(feature = "datafusion", feature = "encryption"))]
pub(crate) const FACTORY_OPT_PLAINTEXT_FOOTER: &str = "plaintext_footer";
#[cfg(all(feature = "datafusion", feature = "encryption"))]
pub(crate) const FACTORY_OPT_COLUMN_KEYS: &str = "column_keys";

/// Parquet Modular Encryption settings from a table's `delta.encryption.*` properties.
///
/// The properties live in the Delta log and apply to every read and write of the table.
/// They are stored in plaintext, so they must hold only key IDs and KMS settings, never
/// keys or credentials. Every build can parse them; encrypting or decrypting data needs the
/// `encryption` cargo feature.
///
/// # Protocol
/// The RFC protects encrypted tables with Reader Version 3, Writer Version 7 and a
/// `parquetEncryption` reader feature. delta-kernel rejects that feature until it supports
/// it, so delta-rs does not add it yet; delta-rs's own protocol checks refuse encrypted
/// tables the build cannot handle instead. Other engines are not protected until then.
#[derive(Debug, Clone)]
pub struct EncryptionConfig {
    /// The KMS client to use (`delta.encryption.kms_id`).
    pub kms_id: String,
    /// Opaque KMS-specific configuration, e.g. JSON (`delta.encryption.kms_configuration`).
    pub kms_configuration: Option<String>,
    /// Master key ID for the footer (`delta.encryption.footer_key`). Always required: see
    /// [`try_from_configuration`](Self::try_from_configuration).
    pub footer_key: String,
    /// Leave the footer unencrypted; defaults to `false` (`delta.encryption.plaintext_footer`).
    pub plaintext_footer: bool,
    /// Master key ID → columns it encrypts (`delta.encryption.column_keys`). Empty means
    /// uniform encryption: every column is encrypted with the footer key.
    pub column_keys: HashMap<String, Vec<String>>,
}

impl EncryptionConfig {
    /// Whether any `delta.encryption.*` property is set, valid or not, in a table's raw
    /// metadata `configuration`. A table with an invalid configuration must still not be
    /// read or written as plaintext.
    pub fn is_configured(configuration: &HashMap<String, String>) -> bool {
        configuration
            .keys()
            .any(|key| key.starts_with(ENCRYPTION_PROP_PREFIX))
    }

    /// Parse and validate the encryption configuration from a table's raw metadata
    /// `configuration` (see [`Metadata::configuration`](delta_kernel::actions::Metadata)).
    ///
    /// Returns `Ok(None)` when no `delta.encryption.*` property is set. Per the RFC,
    /// `footer_key` turns encryption on, so any other encryption property without it is an
    /// error, as are a missing `kms_id`, an unknown `delta.encryption.*` key, and malformed
    /// values.
    ///
    /// # Why the footer key is required
    /// Parquet Modular Encryption always needs a footer key, so there is no mode that
    /// encrypts some columns and leaves the footer alone. By default the footer is
    /// encrypted with it, which hides the schema, row counts, key-value metadata, sort
    /// order, which columns are encrypted and their key metadata; the [Parquet
    /// specification] recommends this whenever any column is sensitive. With
    /// `plaintext_footer` the footer stays readable but is still signed with the footer
    /// key, so tampering is detected. Column keys are therefore an optional refinement
    /// on top of the footer key, never a replacement for it.
    ///
    /// [Parquet specification]: https://parquet.apache.org/docs/file-format/data-pages/encryption/
    pub fn try_from_configuration(
        configuration: &HashMap<String, String>,
    ) -> DeltaResult<Option<Self>> {
        if !Self::is_configured(configuration) {
            return Ok(None);
        }
        let get = |key: &str| {
            configuration
                .get(key)
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
        };

        if let Some(unknown) = configuration.keys().find(|key| {
            key.starts_with(ENCRYPTION_PROP_PREFIX) && !ENCRYPTION_PROPS.contains(&key.as_str())
        }) {
            return Err(invalid_encryption_config(format!(
                "unknown property '{unknown}'; expected one of {ENCRYPTION_PROPS:?}"
            )));
        }
        // Parquet always encrypts or signs the footer with this key; see the doc comment.
        let footer_key = get(ENCRYPTION_FOOTER_KEY_PROP).ok_or_else(|| {
            invalid_encryption_config(format!(
                "'{ENCRYPTION_FOOTER_KEY_PROP}' must be set to enable encryption; Parquet \
                 Modular Encryption always encrypts (or, with \
                 '{ENCRYPTION_PLAINTEXT_FOOTER_PROP}', signs) the footer with it"
            ))
        })?;
        let kms_id = get(ENCRYPTION_KMS_ID_PROP).ok_or_else(|| {
            invalid_encryption_config(format!(
                "'{ENCRYPTION_KMS_ID_PROP}' must be set when '{ENCRYPTION_FOOTER_KEY_PROP}' is set"
            ))
        })?;
        let plaintext_footer = match get(ENCRYPTION_PLAINTEXT_FOOTER_PROP) {
            None => false,
            Some(value) => value.parse::<bool>().map_err(|_| {
                invalid_encryption_config(format!(
                    "'{ENCRYPTION_PLAINTEXT_FOOTER_PROP}' must be 'true' or 'false', got '{value}'"
                ))
            })?,
        };

        Ok(Some(Self {
            kms_id: kms_id.to_string(),
            kms_configuration: get(ENCRYPTION_KMS_CONFIGURATION_PROP).map(str::to_string),
            footer_key: footer_key.to_string(),
            plaintext_footer,
            column_keys: Self::parse_column_keys(get(ENCRYPTION_COLUMN_KEYS_PROP))?,
        }))
    }

    /// Parse `"keyId:col1,col2;keyId2:col3"` into `{keyId: [col1, col2], keyId2: [col3]}`.
    ///
    /// A column may be named only once, and not alongside one of its ancestors: naming a
    /// struct already covers all of its fields.
    fn parse_column_keys(value: Option<&str>) -> DeltaResult<HashMap<String, Vec<String>>> {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        let mut seen: Vec<(String, String)> = Vec::new();
        let Some(value) = value else { return Ok(map) };
        for segment in value.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let malformed = || {
                invalid_encryption_config(format!(
                    "'{ENCRYPTION_COLUMN_KEYS_PROP}' segment '{segment}' must have the form \
                     'keyId:col1,col2'"
                ))
            };
            let (key_id, cols) = segment.split_once(':').ok_or_else(malformed)?;
            let key_id = key_id.trim();
            let cols: Vec<String> = cols
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
            if key_id.is_empty() || cols.is_empty() {
                return Err(malformed());
            }
            for col in &cols {
                if let Some((other, other_key)) = seen
                    .iter()
                    .find(|(other, _)| column_paths_overlap(col, other))
                {
                    return Err(invalid_encryption_config(format!(
                        "column '{col}' (key '{key_id}') overlaps column '{other}' \
                         (key '{other_key}'); each column may be covered by only one key"
                    )));
                }
                seen.push((col.clone(), key_id.to_string()));
            }
            map.entry(key_id.to_string()).or_default().extend(cols);
        }
        Ok(map)
    }

    /// Check [`column_keys`](Self::column_keys) against the table schema: every column must
    /// exist and none may be a partition column. Nested fields are dot-separated
    /// (`address.street`), and with column mapping the names are physical names.
    pub fn validate_columns(
        &self,
        schema: &StructType,
        partition_columns: &[String],
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<()> {
        let physical_partition_columns: Vec<&str> = partition_columns
            .iter()
            .filter_map(|name| schema.field(name))
            .map(|field| field.physical_name(column_mapping_mode))
            .collect();

        for (key_id, columns) in &self.column_keys {
            for column in columns {
                let mut path = column.split('.');
                let top_level = path.next().unwrap_or_default();
                let mut field = schema
                    .fields()
                    .find(|f| f.physical_name(column_mapping_mode) == top_level);
                for part in path {
                    field = match field.map(|f| f.data_type()) {
                        Some(DataType::Struct(inner)) => inner
                            .fields()
                            .find(|f| f.physical_name(column_mapping_mode) == part),
                        _ => None,
                    };
                }
                if field.is_none() {
                    return Err(invalid_encryption_config(format!(
                        "key '{key_id}' names column '{column}', which is not in the table"
                    )));
                }
                if physical_partition_columns.contains(&top_level) {
                    return Err(invalid_encryption_config(format!(
                        "key '{key_id}' names partition column '{column}'; partition columns \
                         cannot be encrypted"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Check that `delta.dataSkippingStatsColumns` asks for no statistics on an encrypted
    /// column.
    ///
    /// The Delta log is plaintext, so the RFC forbids per-file statistics for encrypted
    /// columns in it. The writer leaves them out regardless; this refuses a configuration
    /// that asks for them rather than silently ignoring it. Stats columns are display
    /// names, so with column mapping they are resolved to physical names first; names that
    /// are not in the schema are left for the stats-column validation to report.
    pub fn validate_stats_columns(
        &self,
        configuration: &HashMap<String, String>,
        schema: &StructType,
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<()> {
        let stats_prop = TableProperty::DataSkippingStatsColumns.as_ref();
        let Some(value) = configuration
            .get(stats_prop)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
        else {
            return Ok(());
        };
        let stats_columns = ColumnName::parse_column_name_list(value)
            .map_err(|e| invalid_encryption_config(format!("'{stats_prop}' is malformed: {e}")))?;
        for stats_column in stats_columns {
            let display = stats_column.path().join(".");
            let Some(physical) = physical_path(schema, column_mapping_mode, &display) else {
                continue;
            };
            if self.column_keys.is_empty() {
                return Err(invalid_encryption_config(format!(
                    "'{stats_prop}' names column '{display}', but every column of this table \
                     is encrypted; per-file statistics must not be written to the Delta log \
                     for encrypted columns, so unset '{stats_prop}'"
                )));
            }
            if let Some((key_id, encrypted)) = self
                .column_keys
                .iter()
                .flat_map(|(key_id, cols)| cols.iter().map(move |c| (key_id, c)))
                .find(|(_, encrypted)| column_paths_overlap(&physical, encrypted))
            {
                return Err(invalid_encryption_config(format!(
                    "'{stats_prop}' names column '{display}', which is encrypted with key \
                     '{key_id}' (as '{encrypted}'); per-file statistics must not be written \
                     to the Delta log for encrypted columns"
                )));
            }
        }
        Ok(())
    }

    /// The first property that may not change on an encrypted table and differs between
    /// `self` and `other`, or `None` if the two configurations encrypt the same way.
    ///
    /// Parsed values are compared, so reordering `column_keys` or spelling a default
    /// explicitly is not a change. `kms_configuration` is not compared: it only tells the
    /// KMS client how to reach the keys and may change at any time.
    pub fn changed_frozen_property(&self, other: &Self) -> Option<&'static str> {
        if self.kms_id != other.kms_id {
            Some(ENCRYPTION_KMS_ID_PROP)
        } else if self.footer_key != other.footer_key {
            Some(ENCRYPTION_FOOTER_KEY_PROP)
        } else if self.plaintext_footer != other.plaintext_footer {
            Some(ENCRYPTION_PLAINTEXT_FOOTER_PROP)
        } else if self.column_keys_property() != other.column_keys_property() {
            Some(ENCRYPTION_COLUMN_KEYS_PROP)
        } else {
            None
        }
    }

    /// Rewrite [`column_keys`](Self::column_keys) from display names to physical names.
    ///
    /// The RFC stores physical names so that renaming a column does not invalidate the
    /// configuration. Without column mapping the two are the same and nothing changes.
    pub fn with_physical_column_names(
        mut self,
        schema: &StructType,
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<Self> {
        if column_mapping_mode == ColumnMappingMode::None {
            return Ok(self);
        }
        for (key_id, columns) in self.column_keys.iter_mut() {
            for column in columns.iter_mut() {
                *column = physical_path(schema, column_mapping_mode, column).ok_or_else(|| {
                    invalid_encryption_config(format!(
                        "key '{key_id}' names column '{column}', which is not in the table"
                    ))
                })?;
            }
        }
        Ok(self)
    }

    /// Serialise [`column_keys`](Self::column_keys) back to the
    /// `delta.encryption.column_keys` format, sorted so the result is deterministic.
    pub fn column_keys_property(&self) -> String {
        let mut sorted_keys: Vec<(&String, &Vec<String>)> = self.column_keys.iter().collect();
        sorted_keys.sort_unstable_by_key(|(k, _)| *k);
        sorted_keys
            .iter()
            .map(|(key_id, cols)| {
                let mut sorted_cols: Vec<&str> = cols.iter().map(|s| s.as_str()).collect();
                sorted_cols.sort_unstable();
                format!("{}:{}", key_id, sorted_cols.join(","))
            })
            .collect::<Vec<_>>()
            .join(";")
    }

    /// [`TableParquetOptions`] telling a DataFusion Parquet scan to decrypt with the
    /// factory registered as [`kms_id`](EncryptionConfig::kms_id).
    #[cfg(all(feature = "datafusion", feature = "encryption"))]
    pub fn to_table_parquet_options(&self) -> TableParquetOptions {
        let mut opts = TableParquetOptions::default();
        opts.crypto.factory_id = Some(self.kms_id.clone());
        opts.crypto.factory_options = self.factory_options();
        opts
    }

    /// The options passed to the registered encryption factory.
    #[cfg(all(feature = "datafusion", feature = "encryption"))]
    pub fn factory_options(&self) -> EncryptionFactoryOptions {
        let mut opts = EncryptionFactoryOptions::default();
        if let Some(cfg) = &self.kms_configuration {
            opts.options
                .insert(FACTORY_OPT_KMS_CONFIGURATION.to_string(), cfg.clone());
        }
        opts.options
            .insert(FACTORY_OPT_FOOTER_KEY.to_string(), self.footer_key.clone());
        opts.options.insert(
            FACTORY_OPT_PLAINTEXT_FOOTER.to_string(),
            self.plaintext_footer.to_string(),
        );
        if !self.column_keys.is_empty() {
            // Sorted, so factories can use the string as a cache key.
            opts.options.insert(
                FACTORY_OPT_COLUMN_KEYS.to_string(),
                self.column_keys_property(),
            );
        }
        opts
    }
}

#[cfg(test)]
mod encryption_tests {
    use std::collections::HashMap;

    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::table_features::ColumnMappingMode;

    use super::{
        ENCRYPTION_COLUMN_KEYS_PROP, ENCRYPTION_FOOTER_KEY_PROP, ENCRYPTION_KMS_CONFIGURATION_PROP,
        ENCRYPTION_KMS_ID_PROP, ENCRYPTION_PLAINTEXT_FOOTER_PROP, EncryptionConfig, TableProperty,
    };
    use crate::kernel::transaction::{PROTOCOL, TransactionError};
    use crate::operations::create::CreateBuilder;

    fn props_with(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn try_parse(entries: &[(&str, &str)]) -> Result<Option<EncryptionConfig>, String> {
        EncryptionConfig::try_from_configuration(&props_with(entries)).map_err(|e| e.to_string())
    }

    fn config_with_column_keys(column_keys: &str) -> EncryptionConfig {
        try_parse(&[
            (ENCRYPTION_KMS_ID_PROP, "kms"),
            (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
            (ENCRYPTION_COLUMN_KEYS_PROP, column_keys),
        ])
        .unwrap()
        .unwrap()
    }

    fn schema() -> StructType {
        let address = StructType::try_new([
            StructField::nullable("street", DataType::STRING),
            StructField::nullable("city", DataType::STRING),
        ])
        .unwrap();
        StructType::try_new([
            StructField::nullable("id", DataType::LONG),
            StructField::nullable("ssn", DataType::STRING),
            StructField::nullable("region", DataType::STRING),
            StructField::nullable("address", DataType::Struct(Box::new(address))),
        ])
        .unwrap()
    }

    #[test]
    fn unconfigured_table_is_not_encrypted() {
        assert!(
            try_parse(&[("delta.appendOnly", "true")])
                .unwrap()
                .is_none()
        );
        assert!(!EncryptionConfig::is_configured(&props_with(&[])));
    }

    #[test]
    fn footer_key_is_required() {
        // The RFC makes footer_key the switch that enables encryption, so every other
        // encryption property is an error without it. Parquet has no footer-less mode:
        // column keys only refine what the footer key already protects.
        for prop in [ENCRYPTION_KMS_ID_PROP, ENCRYPTION_COLUMN_KEYS_PROP] {
            let err = try_parse(&[(prop, "kms:col")]).unwrap_err();
            assert!(err.contains(ENCRYPTION_FOOTER_KEY_PROP), "{err}");
        }
    }

    #[test]
    fn kms_id_is_required() {
        let err = try_parse(&[(ENCRYPTION_FOOTER_KEY_PROP, "fk")]).unwrap_err();
        assert!(err.contains(ENCRYPTION_KMS_ID_PROP), "{err}");
    }

    #[test]
    fn unknown_encryption_property_is_rejected() {
        // Catches typos and the pre-RFC dotted names (`delta.encryption.footer.key`).
        let err = try_parse(&[
            (ENCRYPTION_KMS_ID_PROP, "kms"),
            (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
            ("delta.encryption.footer.key", "fk"),
        ])
        .unwrap_err();
        assert!(err.contains("delta.encryption.footer.key"), "{err}");
    }

    #[test]
    fn plaintext_footer_must_be_a_bool() {
        let err = try_parse(&[
            (ENCRYPTION_KMS_ID_PROP, "kms"),
            (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
            (ENCRYPTION_PLAINTEXT_FOOTER_PROP, "yes"),
        ])
        .unwrap_err();
        assert!(err.contains(ENCRYPTION_PLAINTEXT_FOOTER_PROP), "{err}");
    }

    #[test]
    fn minimal_configuration_uses_uniform_encryption() {
        let enc = config_with_column_keys("");
        assert_eq!(enc.kms_id, "kms");
        assert_eq!(enc.footer_key, "fk");
        assert!(enc.kms_configuration.is_none());
        assert!(!enc.plaintext_footer);
        assert!(enc.column_keys.is_empty());
    }

    /// Like every other property, a blank `kms_configuration` means unset, so the KMS
    /// factory is not handed an empty string to parse.
    #[test]
    fn blank_kms_configuration_is_unset() {
        for blank in ["", "  "] {
            let enc = try_parse(&[
                (ENCRYPTION_KMS_ID_PROP, "kms"),
                (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
                (ENCRYPTION_KMS_CONFIGURATION_PROP, blank),
            ])
            .unwrap()
            .unwrap();
            assert!(enc.kms_configuration.is_none(), "{blank:?}");
        }
        let enc = try_parse(&[
            (ENCRYPTION_KMS_ID_PROP, "kms"),
            (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
            (ENCRYPTION_KMS_CONFIGURATION_PROP, " {} "),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(enc.kms_configuration.as_deref(), Some("{}"));
    }

    #[test]
    fn all_fields() {
        let enc = try_parse(&[
            (ENCRYPTION_KMS_ID_PROP, "prod-kms"),
            (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
            (
                "delta.encryption.kms_configuration",
                r#"{"endpoint":"kms.example.com"}"#,
            ),
            (ENCRYPTION_PLAINTEXT_FOOTER_PROP, "true"),
            (ENCRYPTION_COLUMN_KEYS_PROP, "keyA:col1,col2;keyB:col3"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(enc.kms_id, "prod-kms");
        assert_eq!(enc.footer_key, "fk");
        assert_eq!(
            enc.kms_configuration.as_deref(),
            Some(r#"{"endpoint":"kms.example.com"}"#)
        );
        assert!(enc.plaintext_footer);
        assert_eq!(enc.column_keys["keyA"], ["col1", "col2"]);
        assert_eq!(enc.column_keys["keyB"], ["col3"]);
    }

    #[test]
    fn parse_column_keys_trims_whitespace_and_empty_segments() {
        let keys = EncryptionConfig::parse_column_keys(Some(" k1 : col1 , col2 ;; k2:c ")).unwrap();
        assert_eq!(keys["k1"], ["col1", "col2"]);
        assert_eq!(keys["k2"], ["c"]);
        assert!(
            EncryptionConfig::parse_column_keys(None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn parse_column_keys_rejects_malformed_segments() {
        for value in ["col1,col2", ":col1", "k1:", "k1: , "] {
            assert!(
                EncryptionConfig::parse_column_keys(Some(value)).is_err(),
                "{value}"
            );
        }
    }

    #[test]
    fn parse_column_keys_rejects_overlapping_columns() {
        // The same column twice, or a struct together with one of its fields, under the
        // same key or different keys.
        for value in ["k1:a,b;k2:b", "k1:a;k2:a.b", "k1:a.b;k2:a", "k1:a,a.b"] {
            let err = EncryptionConfig::parse_column_keys(Some(value))
                .unwrap_err()
                .to_string();
            assert!(err.contains("overlaps"), "{value}: {err}");
        }
    }

    #[test]
    fn parse_column_keys_accepts_sibling_and_prefix_named_columns() {
        // `ab` and `a.bc` are not inside `a.b`.
        EncryptionConfig::parse_column_keys(Some("k1:a.b;k2:a.bc,ab")).unwrap();
    }

    #[test]
    fn column_keys_property_round_trips_sorted() {
        let enc = config_with_column_keys("k2:c;k1:b,a");
        assert_eq!(enc.column_keys_property(), "k1:a,b;k2:c");
    }

    #[test]
    fn validate_columns_accepts_top_level_and_nested_columns() {
        let enc = config_with_column_keys("pii:ssn,address.street;geo:address.city");
        enc.validate_columns(&schema(), &[], ColumnMappingMode::None)
            .unwrap();
    }

    #[test]
    fn validate_columns_rejects_unknown_columns() {
        for column in ["missing", "address.missing", "ssn.inner"] {
            let err = config_with_column_keys(&format!("pii:{column}"))
                .validate_columns(&schema(), &[], ColumnMappingMode::None)
                .unwrap_err()
                .to_string();
            assert!(err.contains("not in the table"), "{err}");
        }
    }

    #[test]
    fn validate_columns_rejects_partition_columns() {
        let err = config_with_column_keys("pii:region")
            .validate_columns(&schema(), &["region".to_string()], ColumnMappingMode::None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("partition column"), "{err}");
    }

    /// Reordering keys or columns, spelling a default explicitly, whitespace, and a
    /// different KMS endpoint all describe the same encryption; each frozen property is
    /// reported by name when it really changes.
    #[test]
    fn changed_frozen_property_compares_parsed_values() {
        let base_entries = [
            (ENCRYPTION_KMS_ID_PROP, "kms"),
            (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
            (ENCRYPTION_COLUMN_KEYS_PROP, "k1:a,b;k2:c"),
        ];
        let base = try_parse(&base_entries).unwrap().unwrap();
        let same = try_parse(&[
            (ENCRYPTION_KMS_ID_PROP, " kms "),
            (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
            (ENCRYPTION_PLAINTEXT_FOOTER_PROP, "false"),
            (ENCRYPTION_COLUMN_KEYS_PROP, "k2:c; k1:b,a"),
            (ENCRYPTION_KMS_CONFIGURATION_PROP, "{}"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(base.changed_frozen_property(&same), None);

        for (prop, value) in [
            (ENCRYPTION_KMS_ID_PROP, "other"),
            (ENCRYPTION_FOOTER_KEY_PROP, "fk2"),
            (ENCRYPTION_PLAINTEXT_FOOTER_PROP, "true"),
            (ENCRYPTION_COLUMN_KEYS_PROP, "k1:a,b"),
        ] {
            let mut entries: Vec<(&str, &str)> = base_entries
                .iter()
                .copied()
                .filter(|(k, _)| *k != prop)
                .collect();
            entries.push((prop, value));
            let changed = try_parse(&entries).unwrap().unwrap();
            assert_eq!(base.changed_frozen_property(&changed), Some(prop));
            assert_eq!(changed.changed_frozen_property(&base), Some(prop));
        }
    }

    /// Statistics in the Delta log are plaintext, so `delta.dataSkippingStatsColumns` may
    /// not ask for them on an encrypted column, or on any column under uniform encryption.
    #[test]
    fn validate_stats_columns_rejects_encrypted_columns() {
        let stats = |cols: &str| props_with(&[("delta.dataSkippingStatsColumns", cols)]);
        let mode = ColumnMappingMode::None;
        let enc = config_with_column_keys("pii:ssn;geo:address.city");
        enc.validate_stats_columns(&props_with(&[]), &schema(), mode)
            .unwrap();
        enc.validate_stats_columns(
            &stats("id, region, address.street, missing"),
            &schema(),
            mode,
        )
        .unwrap();
        for cols in ["ssn", "id,ssn", "address.city", "address"] {
            let err = enc
                .validate_stats_columns(&stats(cols), &schema(), mode)
                .unwrap_err()
                .to_string();
            assert!(err.contains("encrypted with key"), "{cols}: {err}");
        }
        let err = config_with_column_keys("")
            .validate_stats_columns(&stats("id"), &schema(), mode)
            .unwrap_err()
            .to_string();
        assert!(err.contains("every column"), "{err}");
    }

    /// The `simple_encrypted_table` fixture shows what an encrypted table's metadata looks
    /// like on disk. delta-kernel does not support the `parquetEncryption` table feature
    /// yet, so the log is read directly rather than through `open_table`.
    ///
    /// The configuration is parsed from the raw metadata map and nothing else. Kernel keeps
    /// only the keys it does not recognise in `TableProperties::unknown_properties`, so
    /// reading from there would make every encrypted table look plaintext the day kernel
    /// learns these keys; this test pins the raw-map path.
    #[test]
    fn parses_fixture_table_from_raw_configuration() {
        let log = std::fs::read_to_string(
            "../test/tests/data/simple_encrypted_table/_delta_log/00000000000000000000.json",
        )
        .unwrap();
        let configuration: HashMap<String, String> = log
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find_map(|action| action.get("metaData").cloned())
            .map(|metadata| serde_json::from_value(metadata["configuration"].clone()).unwrap())
            .expect("fixture has a metaData action");

        let enc = EncryptionConfig::try_from_configuration(&configuration)
            .unwrap()
            .expect("should parse");
        assert_eq!(enc.kms_id, "test-kms");
        assert_eq!(enc.footer_key, "footer-key");
        assert_eq!(
            enc.kms_configuration.as_deref(),
            Some(r#"{"endpoint":"https://kms.example.com"}"#)
        );
        assert!(!enc.plaintext_footer);
        assert_eq!(enc.column_keys["pii-key"], ["ssn"]);
        assert_eq!(enc.column_keys["finance-key"], ["salary"]);
    }

    fn create_encrypted_table(column_keys: &str, extra: &[(&str, &str)]) -> CreateBuilder {
        let configuration: HashMap<String, Option<String>> = [
            (ENCRYPTION_KMS_ID_PROP, "test-kms"),
            (ENCRYPTION_FOOTER_KEY_PROP, "footer-key"),
            (ENCRYPTION_COLUMN_KEYS_PROP, column_keys),
        ]
        .iter()
        .chain(extra)
        .map(|(k, v)| (k.to_string(), Some(v.to_string())))
        .collect();
        CreateBuilder::new()
            .with_location("memory:///")
            .with_columns(schema().fields().cloned())
            .with_configuration(configuration)
    }

    /// The encryption keys are typed table properties, so the default unknown-key check
    /// accepts them on every creation path (including the write path, which cannot disable
    /// that check).
    #[test]
    fn encryption_properties_are_typed_table_properties() {
        for (property, key) in [
            (TableProperty::EncryptionKmsId, ENCRYPTION_KMS_ID_PROP),
            (
                TableProperty::EncryptionKmsConfiguration,
                ENCRYPTION_KMS_CONFIGURATION_PROP,
            ),
            (
                TableProperty::EncryptionFooterKey,
                ENCRYPTION_FOOTER_KEY_PROP,
            ),
            (
                TableProperty::EncryptionPlaintextFooter,
                ENCRYPTION_PLAINTEXT_FOOTER_PROP,
            ),
            (
                TableProperty::EncryptionColumnKeys,
                ENCRYPTION_COLUMN_KEYS_PROP,
            ),
        ] {
            assert_eq!(property.as_ref(), key);
            assert!(key.parse::<TableProperty>().unwrap() == property, "{key}");
        }
    }

    /// Files registered at creation (CONVERT TO DELTA, `with_actions`) are plaintext, so
    /// they cannot be committed under an encrypted configuration.
    #[tokio::test]
    async fn create_with_existing_files_rejects_encryption() {
        use crate::kernel::{Action, Add};

        let err = create_encrypted_table("pii-key:ssn", &[])
            .with_actions(vec![Action::Add(Add {
                path: "part-00000.parquet".to_string(),
                data_change: true,
                ..Default::default()
            })])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("existing data files"), "{err}");
    }

    /// Encryption properties written at table creation survive a commit and reload.
    #[tokio::test]
    async fn encryption_properties_round_trip_through_delta_log() {
        let mut table = create_encrypted_table("pii-key:ssn", &[]).await.unwrap();
        table.load().await.unwrap();

        let enc = EncryptionConfig::try_from_configuration(
            table.snapshot().unwrap().metadata().configuration(),
        )
        .unwrap()
        .expect("should parse");
        assert_eq!(enc.kms_id, "test-kms");
        assert_eq!(enc.footer_key, "footer-key");
        assert_eq!(enc.column_keys["pii-key"], ["ssn"]);
    }

    #[tokio::test]
    async fn create_rejects_invalid_encryption_configuration() {
        let err = create_encrypted_table("pii-key:missing", &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("not in the table"), "{err}");

        let err = create_encrypted_table("pii-key:region", &[])
            .with_partition_columns(["region"])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("partition column"), "{err}");

        let err = create_encrypted_table(
            "pii-key:ssn",
            &[("delta.dataSkippingStatsColumns", "id,ssn")],
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("encrypted with key"), "{err}");
    }

    /// With column mapping, display names given at create time are stored as physical names.
    #[tokio::test]
    async fn create_stores_physical_names_under_column_mapping() {
        let table = create_encrypted_table(
            "pii-key:ssn,address.street",
            &[
                ("delta.columnMapping.mode", "name"),
                ("delta.minReaderVersion", "2"),
                ("delta.minWriterVersion", "5"),
            ],
        )
        .await
        .unwrap();
        let snapshot = table.snapshot().unwrap();
        let enc = EncryptionConfig::try_from_configuration(snapshot.metadata().configuration())
            .unwrap()
            .unwrap();
        let columns = &enc.column_keys["pii-key"];

        let physical_schema = snapshot.schema();
        let ssn = physical_schema.field("ssn").unwrap();
        let address = physical_schema.field("address").unwrap();
        let DataType::Struct(address_fields) = address.data_type() else {
            panic!("address is a struct")
        };
        let street = address_fields.field("street").unwrap();
        let expected_ssn = ssn.physical_name(ColumnMappingMode::Name).to_string();
        let expected_street = format!(
            "{}.{}",
            address.physical_name(ColumnMappingMode::Name),
            street.physical_name(ColumnMappingMode::Name)
        );
        assert_ne!(expected_ssn, "ssn");
        assert!(columns.contains(&expected_ssn), "{columns:?}");
        assert!(columns.contains(&expected_street), "{columns:?}");
        enc.validate_columns(&physical_schema, &[], ColumnMappingMode::Name)
            .unwrap();
    }

    fn plaintext_table() -> CreateBuilder {
        CreateBuilder::new()
            .with_location("memory:///")
            .with_columns(schema().fields().cloned())
    }

    fn encryption_properties(footer_key_prop: &str) -> HashMap<String, String> {
        HashMap::from([
            (ENCRYPTION_KMS_ID_PROP.to_string(), "test-kms".to_string()),
            (footer_key_prop.to_string(), "footer-key".to_string()),
        ])
    }

    /// Turning encryption on for an existing table would leave its data files unencrypted.
    #[tokio::test]
    async fn commit_cannot_add_encryption_to_existing_table() {
        let table = plaintext_table().await.unwrap();
        let err = table
            .set_tbl_properties()
            .with_properties(encryption_properties(ENCRYPTION_FOOTER_KEY_PROP))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("only be configured when a table is created"),
            "{err}"
        );
    }

    /// An invalid `delta.encryption.*` property is refused, rather than committed and
    /// leaving a table that delta-rs then refuses to read or write.
    #[tokio::test]
    async fn commit_cannot_add_invalid_encryption_properties() {
        let table = plaintext_table().await.unwrap();
        let err = table
            .set_tbl_properties()
            .with_properties(encryption_properties("delta.encryption.footerkey"))
            .with_raise_if_not_exists(false)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("delta.encryption.footerkey"), "{err}");
    }

    /// Create-or-replace may configure encryption: it replaces all existing data files.
    #[tokio::test]
    async fn create_or_replace_can_add_encryption() {
        let table = plaintext_table().await.unwrap();
        let mut configuration: HashMap<String, Option<String>> =
            encryption_properties(ENCRYPTION_FOOTER_KEY_PROP)
                .into_iter()
                .map(|(k, v)| (k, Some(v)))
                .collect();
        configuration.insert(
            ENCRYPTION_COLUMN_KEYS_PROP.to_string(),
            Some("pii-key:ssn".to_string()),
        );
        let table = CreateBuilder::new()
            .with_log_store(table.log_store())
            .with_columns(schema().fields().cloned())
            .with_configuration(configuration)
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await
            .unwrap();
        assert!(EncryptionConfig::is_configured(
            table.snapshot().unwrap().metadata().configuration()
        ));
    }

    /// Removing encryption would leave a table mixing encrypted and plaintext files, so it is
    /// refused; the data has to be copied into a new table instead.
    #[tokio::test]
    async fn commit_cannot_remove_encryption() {
        use crate::kernel::transaction::CommitBuilder;
        use crate::kernel::{Action, MetadataExt as _};
        use crate::protocol::DeltaOperation;

        let table = create_encrypted_table("pii-key:ssn", &[]).await.unwrap();
        let snapshot = table.snapshot().unwrap().snapshot();
        let mut metadata = snapshot.metadata().clone();
        for key in [
            ENCRYPTION_KMS_ID_PROP,
            ENCRYPTION_FOOTER_KEY_PROP,
            ENCRYPTION_COLUMN_KEYS_PROP,
        ] {
            metadata = metadata.remove_config_key(key).unwrap();
        }
        let err = CommitBuilder::default()
            .with_actions(vec![Action::Metadata(metadata)])
            .build(
                Some(snapshot),
                table.log_store(),
                DeltaOperation::SetTableProperties {
                    properties: HashMap::new(),
                },
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot be removed"), "{err}");
    }

    /// The encryption check runs against the snapshot a transaction read. When a concurrent
    /// commit changed the metadata in the meantime, here a create-or-replace that turned
    /// encryption on, the conflict checker aborts the stale transaction, so a commit checked
    /// against an outdated snapshot cannot strip the configuration just installed.
    #[tokio::test]
    async fn concurrent_metadata_change_aborts_stale_commit() {
        use crate::DeltaTableError;
        use crate::kernel::transaction::{CommitBuilder, CommitConflictError, TransactionError};
        use crate::kernel::{Action, MetadataExt as _};
        use crate::protocol::DeltaOperation;

        let table = plaintext_table().await.unwrap();
        let stale = table.snapshot().unwrap().snapshot().clone();
        let log_store = table.log_store();

        // Another writer replaces the table with an encrypted one.
        let configuration: HashMap<String, Option<String>> =
            encryption_properties(ENCRYPTION_FOOTER_KEY_PROP)
                .into_iter()
                .map(|(k, v)| (k, Some(v)))
                .collect();
        CreateBuilder::new()
            .with_log_store(log_store.clone())
            .with_columns(schema().fields().cloned())
            .with_configuration(configuration)
            .with_save_mode(crate::protocol::SaveMode::Overwrite)
            .await
            .unwrap();

        // A commit built on the stale plaintext snapshot passes the encryption check
        // (plaintext to plaintext) but would remove the encryption just committed.
        let metadata = stale
            .metadata()
            .clone()
            .with_description("stale".to_string())
            .unwrap();
        let err = CommitBuilder::default()
            .with_actions(vec![Action::Metadata(metadata)])
            .build(
                Some(&stale),
                log_store,
                DeltaOperation::SetTableProperties {
                    properties: HashMap::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                DeltaTableError::Transaction {
                    source: TransactionError::CommitConflict(CommitConflictError::MetadataChanged)
                }
            ),
            "{err:?}"
        );
    }

    /// Until the encryption read and write paths land, delta-rs refuses encrypted tables
    /// instead of reading ciphertext or writing plaintext into them.
    #[tokio::test]
    async fn protocol_checker_refuses_encrypted_tables() {
        let table = create_encrypted_table("pii-key:ssn", &[]).await.unwrap();
        let snapshot = table.snapshot().unwrap().snapshot();
        for result in [
            PROTOCOL.can_read_from(snapshot),
            PROTOCOL.can_write_to(snapshot),
        ] {
            match result {
                Err(TransactionError::UnsupportedTableFeatures(features)) => {
                    assert_eq!(features.len(), 1);
                    assert!(format!("{features:?}").contains("parquetEncryption"));
                }
                other => panic!("expected UnsupportedTableFeatures, got {other:?}"),
            }
        }
    }
}
