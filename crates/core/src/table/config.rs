//! Delta Table configuration
use std::collections::HashMap;
use std::num::{NonZero, NonZeroU64};
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

#[cfg(all(feature = "datafusion", feature = "encryption"))]
use datafusion::config::{EncryptionFactoryOptions, TableParquetOptions};
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
// Property names and semantics follow the Parquet encryption protocol RFC:
// https://github.com/delta-io/delta/issues/6195
//
// delta-kernel does not know these properties yet, so it leaves them in
// `TableProperties::unknown_properties` (the catch-all for unrecognised keys,
// also used for `delta.constraints.*`). We read them from there until support
// for them is added to delta-kernel, at which point this should switch to the
// typed fields.

/// Prefix shared by all encryption table properties.
pub const ENCRYPTION_PROP_PREFIX: &str = "delta.encryption.";
/// Table property naming the KMS client used to encrypt the table.
pub const ENCRYPTION_KMS_ID_PROP: &str = "delta.encryption.kms_id";
/// Table property holding opaque, KMS-specific configuration.
pub const ENCRYPTION_KMS_CONFIGURATION_PROP: &str = "delta.encryption.kms_configuration";
/// Table property holding the master key ID used for footer encryption.
pub const ENCRYPTION_FOOTER_KEY_PROP: &str = "delta.encryption.footer_key";
/// Table property controlling whether parquet footers are left unencrypted.
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

fn invalid_encryption_config(msg: String) -> DeltaTableError {
    DeltaTableError::Generic(format!("Invalid table encryption configuration: {msg}"))
}

/// Key names forwarded to [`EncryptionFactoryOptions`] (suffix after `delta.encryption.` stripped).
#[cfg(all(feature = "datafusion", feature = "encryption"))]
pub(crate) const FACTORY_OPT_KMS_CONFIGURATION: &str = "kms_configuration";
#[cfg(all(feature = "datafusion", feature = "encryption"))]
pub(crate) const FACTORY_OPT_FOOTER_KEY: &str = "footer_key";
#[cfg(all(feature = "datafusion", feature = "encryption"))]
pub(crate) const FACTORY_OPT_PLAINTEXT_FOOTER: &str = "plaintext_footer";
#[cfg(all(feature = "datafusion", feature = "encryption"))]
pub(crate) const FACTORY_OPT_COLUMN_KEYS: &str = "column_keys";

/// Parquet encryption configuration derived from `delta.encryption.*` table properties.
///
/// These properties are stored in the Delta log metadata and are automatically applied to
/// all read and write operations — no per-operation configuration is needed.
///
/// Parsing is always available so that every build can recognise an encrypted table;
/// actually encrypting or decrypting data requires the `encryption` cargo feature.
///
/// The properties are stored in plaintext in the Delta log, so they must only hold key
/// *identifiers* and KMS settings — never key material or credentials.
///
/// # Protocol
/// The RFC requires Reader Version 3, Writer Version 7 and a `parquetEncryption` reader
/// feature, so that engines without encryption support refuse the table. delta-kernel
/// rejects that feature until it gains support, so for now delta-rs does not add it;
/// instead delta-rs's own protocol checks refuse any table with `delta.encryption.*`
/// properties that the build cannot handle. Engines other than delta-rs are not protected
/// until the feature is added. Data files use Parquet Modular Encryption (parquet-format 2.7+).
///
/// # Registering a KMS client
/// Before operating on an encrypted table, register an [`EncryptionFactory`] whose ID
/// matches `delta.encryption.kms_id` with DataFusion's `RuntimeEnv`:
///
/// ```rust,ignore
/// session.runtime_env().register_parquet_encryption_factory("my-kms", factory);
/// ```
///
/// [`EncryptionFactory`]: datafusion::execution::parquet_encryption::EncryptionFactory
#[derive(Debug, Clone)]
pub struct EncryptionConfig {
    /// Identifies the `EncryptionFactory` registered in DataFusion's `RuntimeEnv`.
    /// Corresponds to `delta.encryption.kms_id`.
    pub kms_id: String,
    /// Opaque KMS-specific configuration string (e.g. JSON) forwarded to the factory.
    /// Corresponds to `delta.encryption.kms_configuration`.
    pub kms_configuration: Option<String>,
    /// Master key identifier for footer encryption.
    /// Corresponds to `delta.encryption.footer_key`.
    pub footer_key: String,
    /// If `true` the parquet footer is left unencrypted (plaintext footer mode).
    /// Defaults to `false`. Corresponds to `delta.encryption.plaintext_footer`.
    pub plaintext_footer: bool,
    /// Per-column encryption: map from master key identifier → list of column names.
    /// Empty means uniform encryption: every column is encrypted with the footer key.
    /// Stored in the natural wire format (`keyId → [col1, col2]`) so serialisation is
    /// a direct forward pass with no inversion.
    /// Corresponds to `delta.encryption.column_keys` with format `keyId:col1,col2;keyId2:col3`.
    pub column_keys: HashMap<String, Vec<String>>,
}

impl EncryptionConfig {
    /// Whether the table has any `delta.encryption.*` property set, valid or not.
    ///
    /// Use this to decide whether a table must be treated as encrypted: a table with a
    /// broken configuration is still not safe to read or write as plaintext.
    pub fn is_configured(props: &TableProperties) -> bool {
        props
            .unknown_properties
            .keys()
            .any(|key| key.starts_with(ENCRYPTION_PROP_PREFIX))
    }

    /// Parse encryption configuration from a table's `unknown_properties`
    /// (see the module note above on why these are not typed kernel properties).
    ///
    /// Returns `None` if the table is not encrypted or its configuration is invalid. Use
    /// [`try_from_properties`](Self::try_from_properties) to get the reason instead.
    pub fn from_properties(props: &TableProperties) -> Option<Self> {
        Self::try_from_properties(props).ok().flatten()
    }

    /// Parse and validate encryption configuration from a table's properties.
    ///
    /// Returns `Ok(None)` when no `delta.encryption.*` property is set. Per the RFC, the
    /// presence of `delta.encryption.footer_key` is what turns encryption on, so any other
    /// encryption property set without it is an error, as is a missing `kms_id`, an unknown
    /// `delta.encryption.*` key, or a malformed value.
    pub fn try_from_properties(props: &TableProperties) -> DeltaResult<Option<Self>> {
        if !Self::is_configured(props) {
            return Ok(None);
        }
        let get = |key: &str| {
            props
                .unknown_properties
                .get(key)
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
        };

        if let Some(unknown) = props.unknown_properties.keys().find(|key| {
            key.starts_with(ENCRYPTION_PROP_PREFIX) && !ENCRYPTION_PROPS.contains(&key.as_str())
        }) {
            return Err(invalid_encryption_config(format!(
                "unknown property '{unknown}'; expected one of {ENCRYPTION_PROPS:?}"
            )));
        }
        let footer_key = get(ENCRYPTION_FOOTER_KEY_PROP).ok_or_else(|| {
            invalid_encryption_config(format!(
                "'{ENCRYPTION_FOOTER_KEY_PROP}' must be set to enable encryption"
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
            kms_configuration: props
                .unknown_properties
                .get(ENCRYPTION_KMS_CONFIGURATION_PROP)
                .cloned(),
            footer_key: footer_key.to_string(),
            plaintext_footer,
            column_keys: Self::parse_column_keys(get(ENCRYPTION_COLUMN_KEYS_PROP))?,
        }))
    }

    /// Parse `"keyId:col1,col2;keyId2:col3"` into `{keyId: [col1, col2], keyId2: [col3]}`.
    ///
    /// Each column may be assigned to only one key.
    fn parse_column_keys(value: Option<&str>) -> DeltaResult<HashMap<String, Vec<String>>> {
        let mut map: HashMap<String, Vec<String>> = HashMap::new();
        let mut seen: HashMap<String, String> = HashMap::new();
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
                if let Some(other) = seen.insert(col.clone(), key_id.to_string()) {
                    return Err(invalid_encryption_config(format!(
                        "column '{col}' is assigned to both key '{other}' and key '{key_id}'"
                    )));
                }
            }
            map.entry(key_id.to_string()).or_default().extend(cols);
        }
        Ok(map)
    }

    /// Check [`column_keys`](Self::column_keys) against the table schema.
    ///
    /// Every column must exist and none may be a partition column. Nested fields are named
    /// with dots (`address.street`); naming a struct column covers all of its fields. When
    /// column mapping is enabled, names must be the *physical* column names.
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
                let mut fields = Some(schema);
                let mut physical = Vec::new();
                for part in column.split('.') {
                    let field = fields.and_then(|f| f.field(part)).ok_or_else(|| {
                        invalid_encryption_config(format!(
                            "key '{key_id}' names column '{column}', which is not in the table"
                        ))
                    })?;
                    physical.push(field.physical_name(column_mapping_mode).to_string());
                    fields = match field.data_type() {
                        DataType::Struct(inner) => Some(inner.as_ref()),
                        _ => None,
                    };
                }
                *column = physical.join(".");
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

    /// Build a [`TableParquetOptions`] that tells DataFusion's parquet scan to look up the
    /// decryption factory by [`kms_id`](EncryptionConfig::kms_id) in the `RuntimeEnv`.
    #[cfg(all(feature = "datafusion", feature = "encryption"))]
    pub fn to_table_parquet_options(&self) -> TableParquetOptions {
        let mut opts = TableParquetOptions::default();
        opts.crypto.factory_id = Some(self.kms_id.clone());
        opts.crypto.factory_options = self.factory_options();
        opts
    }

    /// Build [`EncryptionFactoryOptions`] forwarded to the registered factory.
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
            // Sorted so the string is deterministic (factories may use it as a cache key).
            opts.options.insert(
                FACTORY_OPT_COLUMN_KEYS.to_string(),
                self.column_keys_property(),
            );
        }
        opts
    }
}

/// Extension method for conveniently reading encryption config from any `TableProperties`.
pub trait EncryptionExt {
    /// Parse the table's encryption configuration, or `None` if the table is not encrypted.
    fn encryption_config(&self) -> Option<EncryptionConfig>;
}

impl EncryptionExt for TableProperties {
    fn encryption_config(&self) -> Option<EncryptionConfig> {
        EncryptionConfig::from_properties(self)
    }
}

#[cfg(test)]
mod encryption_tests {
    use std::collections::HashMap;

    use delta_kernel::schema::{DataType, StructField, StructType};
    use delta_kernel::table_features::ColumnMappingMode;
    use delta_kernel::table_properties::TableProperties;

    use super::{
        ENCRYPTION_COLUMN_KEYS_PROP, ENCRYPTION_FOOTER_KEY_PROP, ENCRYPTION_KMS_ID_PROP,
        ENCRYPTION_PLAINTEXT_FOOTER_PROP, EncryptionConfig,
    };
    use crate::kernel::transaction::{PROTOCOL, TransactionError};
    use crate::operations::create::CreateBuilder;

    fn props_with(entries: &[(&str, &str)]) -> TableProperties {
        TableProperties::from(entries.iter().copied())
    }

    fn try_parse(entries: &[(&str, &str)]) -> Result<Option<EncryptionConfig>, String> {
        EncryptionConfig::try_from_properties(&props_with(entries)).map_err(|e| e.to_string())
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
        // encryption property is an error without it.
        for prop in [ENCRYPTION_KMS_ID_PROP, ENCRYPTION_COLUMN_KEYS_PROP] {
            let err = try_parse(&[(prop, "kms:col")]).unwrap_err();
            assert!(err.contains(ENCRYPTION_FOOTER_KEY_PROP), "{err}");
        }
        assert!(
            EncryptionConfig::from_properties(&props_with(&[(ENCRYPTION_KMS_ID_PROP, "kms")]))
                .is_none()
        );
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
    fn parse_column_keys_rejects_column_under_two_keys() {
        let err = EncryptionConfig::parse_column_keys(Some("k1:a,b;k2:b"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("'b'"), "{err}");
    }

    #[test]
    fn column_keys_property_round_trips_sorted() {
        let enc = config_with_column_keys("k2:c;k1:b,a");
        assert_eq!(enc.column_keys_property(), "k1:a,b;k2:c");
    }

    #[test]
    fn validate_columns_accepts_top_level_and_nested_columns() {
        let enc = config_with_column_keys("pii:ssn,address.street;geo:address");
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

    /// The `simple_encrypted_table` fixture shows what an encrypted table's metadata looks
    /// like on disk. delta-kernel does not support the `parquetEncryption` table feature
    /// yet, so the log is read directly rather than through `open_table`.
    #[test]
    fn from_properties_parses_fixture_table_metadata() {
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

        let props = TableProperties::from(configuration);
        let enc = EncryptionConfig::try_from_properties(&props)
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
            // delta.encryption.* keys are not in the TableProperty enum yet.
            .with_raise_if_key_not_exists(false)
            .with_configuration(configuration)
    }

    /// Encryption properties written at table creation survive a commit and reload.
    #[tokio::test]
    async fn encryption_properties_round_trip_through_delta_log() {
        let mut table = create_encrypted_table("pii-key:ssn", &[]).await.unwrap();
        table.load().await.unwrap();

        let enc = EncryptionConfig::from_properties(table.snapshot().unwrap().table_config())
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
        let enc = EncryptionConfig::from_properties(snapshot.table_config()).unwrap();
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
