use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use delta_kernel::table_features::{ColumnMappingMode, TableFeature};
use delta_kernel::table_properties::TableProperties;

use super::{TableReference, TransactionError};
#[cfg(feature = "nanosecond-timestamps")]
use crate::kernel::contains_timestamp_nanos;
use crate::kernel::{
    Action, EagerSnapshot, Metadata, Protocol, ProtocolExt as _, Schema, contains_timestampntz,
    contains_variant,
};
use crate::protocol::DeltaOperation;
use crate::table::config::{
    ENCRYPTION_COLUMN_KEYS_PROP, ENCRYPTION_FOOTER_KEY_PROP, ENCRYPTION_KMS_ID_PROP,
    ENCRYPTION_PLAINTEXT_FOOTER_PROP, EncryptionConfig, TablePropertiesExt as _,
};

use tracing::log::*;

/// The table feature the encryption RFC (delta-io/delta#6195) defines.
const PARQUET_ENCRYPTION_FEATURE: &str = "parquetEncryption";

/// Whether this build can read and write tables with `delta.encryption.*` properties.
/// Both need `datafusion` + `encryption`; other builds refuse such tables, so readers never
/// get ciphertext and writers never add plaintext files.
const READS_ENCRYPTED_TABLES: bool = cfg!(all(feature = "datafusion", feature = "encryption"));
const WRITES_ENCRYPTED_TABLES: bool = cfg!(all(feature = "datafusion", feature = "encryption"));

static READER_V2: LazyLock<HashSet<TableFeature>> =
    LazyLock::new(|| HashSet::from_iter([TableFeature::ColumnMapping]));
static READER_V3: LazyLock<HashSet<TableFeature>> =
    LazyLock::new(|| HashSet::from_iter([TableFeature::DeletionVectors]));
#[cfg(feature = "datafusion")]
static WRITER_V2: LazyLock<HashSet<TableFeature>> =
    LazyLock::new(|| HashSet::from_iter([TableFeature::AppendOnly, TableFeature::Invariants]));
// Invariants cannot work in the default builds where datafusion is not present currently, this
// feature configuration ensures that the writer doesn't pretend otherwise
#[cfg(not(feature = "datafusion"))]
static WRITER_V2: LazyLock<HashSet<TableFeature>> =
    LazyLock::new(|| HashSet::from_iter([TableFeature::AppendOnly]));
static WRITER_V3: LazyLock<HashSet<TableFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        TableFeature::AppendOnly,
        TableFeature::Invariants,
        TableFeature::CheckConstraints,
    ])
});
static WRITER_V4: LazyLock<HashSet<TableFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        TableFeature::AppendOnly,
        TableFeature::Invariants,
        TableFeature::CheckConstraints,
        TableFeature::ChangeDataFeed,
        TableFeature::GeneratedColumns,
    ])
});
static WRITER_V5: LazyLock<HashSet<TableFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        TableFeature::AppendOnly,
        TableFeature::Invariants,
        TableFeature::CheckConstraints,
        TableFeature::ChangeDataFeed,
        TableFeature::GeneratedColumns,
        TableFeature::ColumnMapping,
    ])
});
static WRITER_V6: LazyLock<HashSet<TableFeature>> = LazyLock::new(|| {
    HashSet::from_iter([
        TableFeature::AppendOnly,
        TableFeature::Invariants,
        TableFeature::CheckConstraints,
        TableFeature::ChangeDataFeed,
        TableFeature::GeneratedColumns,
        TableFeature::ColumnMapping,
        TableFeature::IdentityColumns,
    ])
});

pub struct ProtocolChecker {
    reader_features: HashSet<TableFeature>,
    writer_features: HashSet<TableFeature>,
}

impl ProtocolChecker {
    /// Create a new protocol checker.
    pub fn new(
        reader_features: HashSet<TableFeature>,
        writer_features: HashSet<TableFeature>,
    ) -> Self {
        Self {
            reader_features,
            writer_features,
        }
    }

    pub fn default_reader_version(&self) -> i32 {
        1
    }

    pub fn default_writer_version(&self) -> i32 {
        2
    }

    /// Check append-only at the high level (operation level)
    pub fn check_append_only(&self, snapshot: &EagerSnapshot) -> Result<(), TransactionError> {
        if snapshot.table_properties().append_only() {
            return Err(TransactionError::DeltaTableAppendOnly);
        }
        Ok(())
    }

    fn check_can_write_feature(
        &self,
        snapshot: &EagerSnapshot,
        contains_feature: bool,
        feature: TableFeature,
    ) -> Result<(), TransactionError> {
        let required_features: Option<&[TableFeature]> =
            match snapshot.protocol().min_writer_version() {
                0..=6 => None,
                _ => snapshot.protocol().writer_features(),
            };

        if let Some(table_features) = required_features {
            if !table_features.contains(&feature) && contains_feature {
                return Err(TransactionError::TableFeaturesRequired(feature));
            }
        } else if contains_feature {
            return Err(TransactionError::TableFeaturesRequired(feature));
        }
        Ok(())
    }

    /// Check can write_timestamp_ntz
    pub fn check_can_write_timestamp_ntz(
        &self,
        snapshot: &EagerSnapshot,
        schema: &Schema,
    ) -> Result<(), TransactionError> {
        trace!(
            "checking if snapshot v{} can write timestampntz",
            snapshot.version()
        );
        self.check_can_write_feature(
            snapshot,
            contains_timestampntz(schema.fields()),
            TableFeature::TimestampWithoutTimezone,
        )
    }

    #[cfg(feature = "nanosecond-timestamps")]
    /// Check can write_timestamp_nanos.
    /// Requires both timestampNanos and timestampNtz features.
    pub fn check_can_write_timestamp_nanos(
        &self,
        snapshot: &EagerSnapshot,
        schema: &Schema,
    ) -> Result<(), TransactionError> {
        trace!(
            "checking if snapshot v{} can write timestampnanos",
            snapshot.version()
        );
        let contains_nanos = contains_timestamp_nanos(schema.fields());
        self.check_can_write_feature(snapshot, contains_nanos, TableFeature::TimestampNanos)?;
        self.check_can_write_feature(
            snapshot,
            contains_nanos,
            TableFeature::TimestampWithoutTimezone,
        )
    }

    /// Check can write variant
    pub fn check_can_write_variant(
        &self,
        snapshot: &EagerSnapshot,
        schema: &Schema,
    ) -> Result<(), TransactionError> {
        trace!(
            "checking if snapshot v{} can write variant",
            snapshot.version()
        );
        let contains_variant = contains_variant(schema.fields());
        let required_features: Option<&[TableFeature]> =
            match snapshot.protocol().min_writer_version() {
                0..=6 => None,
                _ => snapshot.protocol().writer_features(),
            };

        let has_variant_feature = |features: &[TableFeature]| {
            features.contains(&TableFeature::VariantType)
                || features.contains(&TableFeature::VariantTypePreview)
        };

        if let Some(table_features) = required_features {
            if !has_variant_feature(table_features) && contains_variant {
                return Err(TransactionError::TableFeaturesRequired(
                    TableFeature::VariantType,
                ));
            }
        } else if contains_variant {
            return Err(TransactionError::TableFeaturesRequired(
                TableFeature::VariantType,
            ));
        }

        Ok(())
    }

    /// Check if delta-rs can read form the given delta table.
    pub fn can_read_from(&self, snapshot: &dyn TableReference) -> Result<(), TransactionError> {
        self.can_read_from_protocol(snapshot.protocol())?;
        self.check_encryption(snapshot.config(), READS_ENCRYPTED_TABLES)
    }

    /// Check that this build can read a table with these properties.
    ///
    /// delta-kernel cannot open tables with the RFC's `parquetEncryption` feature yet, so
    /// encrypted tables are recognised by their `delta.encryption.*` properties and refused
    /// with the error the feature would produce.
    pub fn can_read_encryption(&self, config: &TableProperties) -> Result<(), TransactionError> {
        self.check_encryption(config, READS_ENCRYPTED_TABLES)
    }

    fn check_encryption(
        &self,
        config: &TableProperties,
        supported: bool,
    ) -> Result<(), TransactionError> {
        if !supported && EncryptionConfig::is_configured(config) {
            return Err(TransactionError::UnsupportedTableFeatures(vec![
                TableFeature::Unknown(PARQUET_ENCRYPTION_FEATURE.to_string()),
            ]));
        }
        Ok(())
    }

    pub fn can_read_from_protocol(&self, protocol: &Protocol) -> Result<(), TransactionError> {
        trace!(
            "validating that min reader version {} can be read",
            protocol.min_reader_version()
        );

        let required_features: Option<HashSet<TableFeature>> = match protocol.min_reader_version() {
            0 | 1 => None,
            2 => Some(READER_V2.clone()),
            3 => protocol
                .reader_features_set()
                .or_else(|| Some(READER_V3.clone())),
            _ => protocol.reader_features_set(),
        };
        trace!("my reader features: {:?}", self.reader_features);
        trace!("desired reader features: {required_features:?}");
        if let Some(features) = required_features {
            let mut diff = features.difference(&self.reader_features).peekable();
            if diff.peek().is_some() {
                return Err(TransactionError::UnsupportedTableFeatures(
                    diff.cloned().collect(),
                ));
            }
        };
        Ok(())
    }

    /// Check if delta-rs can write to the given delta table.
    pub fn can_write_to(&self, snapshot: &dyn TableReference) -> Result<(), TransactionError> {
        // NOTE: writers must always support all required reader features. Encryption is
        // checked separately: writing an encrypted table needs write support only, and an
        // operation that also reads data files fails on them without read support.
        self.can_read_from_protocol(snapshot.protocol())?;
        self.check_encryption(snapshot.config(), WRITES_ENCRYPTED_TABLES)?;
        let min_writer_version = snapshot.protocol().min_writer_version();

        let required_features: Option<HashSet<TableFeature>> = match min_writer_version {
            0 | 1 => None,
            2 => Some(WRITER_V2.clone()),
            3 => Some(WRITER_V3.clone()),
            4 => Some(WRITER_V4.clone()),
            5 => Some(WRITER_V5.clone()),
            6 => Some(WRITER_V6.clone()),
            _ => snapshot.protocol().writer_features_set(),
        };

        trace!("my writer features: {:?}", self.writer_features);
        trace!("required writer features: {required_features:?}");

        if let Some(features) = required_features {
            let mut diff = features.difference(&self.writer_features).peekable();
            if diff.peek().is_some() {
                return Err(TransactionError::UnsupportedTableFeatures(
                    diff.cloned().collect(),
                ));
            }
        };
        Ok(())
    }

    pub fn can_commit(
        &self,
        snapshot: &dyn TableReference,
        actions: &[Action],
        operation: &DeltaOperation,
    ) -> Result<(), TransactionError> {
        let new_metadata = actions.iter().find_map(|action| match action {
            Action::Metadata(metadata) => Some(metadata),
            _ => None,
        });
        if let Some(metadata) = new_metadata {
            check_encryption_change(snapshot, metadata, operation)?;
        }
        self.can_write_to(snapshot)?;

        // https://github.com/delta-io/delta/blob/master/PROTOCOL.md#append-only-tables
        let append_only_enabled = if snapshot.protocol().min_writer_version() < 2 {
            false
        } else if snapshot.protocol().min_writer_version() < 7 {
            snapshot.config().append_only()
        } else {
            snapshot
                .protocol()
                .writer_features()
                .ok_or(TransactionError::TableFeaturesRequired(
                    TableFeature::AppendOnly,
                ))?
                .contains(&TableFeature::AppendOnly)
                && snapshot.config().append_only()
        };
        if append_only_enabled {
            match operation {
                DeltaOperation::Restore { .. } | DeltaOperation::FileSystemCheck { .. } => {}
                _ => {
                    actions.iter().try_for_each(|action| match action {
                        Action::Remove(remove) if remove.data_change => {
                            Err(TransactionError::DeltaTableAppendOnly)
                        }
                        _ => Ok(()),
                    })?;
                }
            }
        }

        Ok(())
    }
}

/// The global protocol checker instance to validate table versions and features.
///
/// This instance is used by default in all transaction operations, since feature
/// support is not configurable but rather decided at compile time.
///
/// As we implement new features, we need to update this instance accordingly.
/// resulting version support is determined by the supported table feature set.
pub static INSTANCE: LazyLock<ProtocolChecker> = LazyLock::new(|| {
    let mut reader_features = HashSet::new();
    reader_features.insert(TableFeature::TimestampWithoutTimezone);
    reader_features.insert(TableFeature::DeletionVectors);
    reader_features.insert(TableFeature::VariantType);
    reader_features.insert(TableFeature::VariantTypePreview);
    reader_features.insert(TableFeature::V2Checkpoint);
    #[cfg(feature = "nanosecond-timestamps")]
    reader_features.insert(TableFeature::TimestampNanos);
    #[cfg(feature = "datafusion")]
    {
        reader_features.insert(TableFeature::ColumnMapping);
    }

    let mut writer_features = HashSet::new();
    writer_features.insert(TableFeature::AppendOnly);
    writer_features.insert(TableFeature::TimestampWithoutTimezone);
    #[cfg(feature = "nanosecond-timestamps")]
    writer_features.insert(TableFeature::TimestampNanos);
    writer_features.insert(TableFeature::VariantType);
    writer_features.insert(TableFeature::VariantTypePreview);
    writer_features.insert(TableFeature::V2Checkpoint);
    #[cfg(feature = "datafusion")]
    {
        writer_features.insert(TableFeature::ChangeDataFeed);
        writer_features.insert(TableFeature::Invariants);
        writer_features.insert(TableFeature::CheckConstraints);
        writer_features.insert(TableFeature::GeneratedColumns);
        writer_features.insert(TableFeature::ColumnMapping);
    }
    writer_features.insert(TableFeature::DeletionVectors);
    // writer_features.insert(TableFeature::IdentityColumns);

    ProtocolChecker::new(reader_features, writer_features)
});

/// Check the encryption configuration a commit installs on an existing table.
///
/// Encryption can only be configured when the table is created (including create-or-replace,
/// which replaces every data file): turning it on later would leave the existing files
/// unencrypted, removing it would leave a table mixing encrypted and plaintext files, and
/// changing the keys would leave files encrypted under different keys. Restore re-installs
/// an earlier version's metadata together with its data files, so it may change any of them.
/// `kms_configuration` only tells the KMS client how to reach the keys, so it may change at
/// any time. A new configuration must also be valid.
fn check_encryption_change(
    snapshot: &dyn TableReference,
    metadata: &Metadata,
    operation: &DeltaOperation,
) -> Result<(), TransactionError> {
    let invalid =
        |err: crate::DeltaTableError| TransactionError::InvalidEncryptionConfig(err.to_string());
    let properties = TableProperties::from(metadata.configuration().iter());
    let replaces_files = matches!(
        operation,
        DeltaOperation::Create { .. } | DeltaOperation::Restore { .. }
    );
    let was_encrypted = EncryptionConfig::is_configured(snapshot.config());
    if was_encrypted && !replaces_files && !EncryptionConfig::is_configured(&properties) {
        return Err(TransactionError::InvalidEncryptionConfig(
            "Encryption properties cannot be removed from a table; to stop encrypting, copy \
             the data into a new table without them"
                .to_string(),
        ));
    }
    let Some(encryption) = EncryptionConfig::try_from_properties(&properties).map_err(invalid)?
    else {
        return Ok(());
    };
    if !was_encrypted && !replaces_files {
        return Err(TransactionError::InvalidEncryptionConfig(
            "Encryption can only be configured when a table is created; the data files \
             already in this table would stay unencrypted"
                .to_string(),
        ));
    }
    if !replaces_files {
        let old = &snapshot.config().unknown_properties;
        let new = &properties.unknown_properties;
        let value = |props: &HashMap<String, String>, key: &str| {
            props.get(key).map(|v| v.trim().to_string())
        };
        if let Some(changed) = [
            ENCRYPTION_KMS_ID_PROP,
            ENCRYPTION_FOOTER_KEY_PROP,
            ENCRYPTION_PLAINTEXT_FOOTER_PROP,
            ENCRYPTION_COLUMN_KEYS_PROP,
        ]
        .into_iter()
        .find(|key| value(old, key) != value(new, key))
        {
            return Err(TransactionError::InvalidEncryptionConfig(format!(
                "'{changed}' cannot be changed on an encrypted table; the data files already \
                 in this table would stay encrypted under the old configuration"
            )));
        }
    }
    let schema = metadata.parse_schema().map_err(|err| invalid(err.into()))?;
    encryption
        .validate_columns(
            &schema,
            metadata.partition_columns(),
            properties
                .column_mapping_mode
                .unwrap_or(ColumnMappingMode::None),
        )
        .map_err(invalid)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::TableProperty;
    use crate::kernel::DataType as DeltaDataType;
    use crate::kernel::{Action, Add, Metadata, PrimitiveType, ProtocolInner, Remove, StructField};
    use crate::protocol::SaveMode;
    use crate::table::state::DeltaTableState;
    use crate::test_utils::{ActionFactory, TestSchemas};

    fn metadata_action(configuration: Option<HashMap<String, Option<String>>>) -> Metadata {
        ActionFactory::metadata(TestSchemas::simple(), None::<Vec<&str>>, configuration)
    }

    #[tokio::test]
    async fn test_can_commit_append_only() {
        let append_actions = vec![Action::Add(Add {
            path: "test".to_string(),
            data_change: true,
            ..Default::default()
        })];
        let append_op = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };

        let change_actions = vec![
            Action::Add(Add {
                path: "test".to_string(),
                data_change: true,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "test".to_string(),
                data_change: true,
                ..Default::default()
            }),
        ];
        let change_op = DeltaOperation::Update { predicate: None };

        let neutral_actions = vec![
            Action::Add(Add {
                path: "test".to_string(),
                data_change: false,
                ..Default::default()
            }),
            Action::Remove(Remove {
                path: "test".to_string(),
                data_change: false,
                ..Default::default()
            }),
        ];
        let neutral_op = DeltaOperation::Update { predicate: None };

        let create_actions = |writer: i32, append: &str, feat: Vec<TableFeature>| {
            vec![
                Action::Protocol(
                    ProtocolInner {
                        min_reader_version: 1,
                        min_writer_version: writer,
                        writer_features: if writer == 7 {
                            Some(feat.into_iter().collect())
                        } else if feat.is_empty() {
                            None
                        } else {
                            Some(feat.into_iter().collect())
                        },
                        ..Default::default()
                    }
                    .as_kernel(),
                ),
                metadata_action(Some(HashMap::from([(
                    TableProperty::AppendOnly.as_ref().to_string(),
                    Some(append.to_string()),
                )])))
                .into(),
            ]
        };

        let checker = ProtocolChecker::new(HashSet::new(), WRITER_V2.clone());

        let actions = create_actions(1, "true", vec![]);
        let snapshot = DeltaTableState::from_actions(actions).await.unwrap();
        let eager = snapshot.snapshot();
        assert!(
            checker
                .can_commit(eager, &append_actions, &append_op)
                .is_ok()
        );
        assert!(
            checker
                .can_commit(eager, &change_actions, &change_op)
                .is_ok()
        );
        assert!(
            checker
                .can_commit(eager, &neutral_actions, &neutral_op)
                .is_ok()
        );

        let actions = create_actions(2, "true", vec![]);
        let snapshot = DeltaTableState::from_actions(actions).await.unwrap();
        let eager = snapshot.snapshot();
        assert!(
            checker
                .can_commit(eager, &append_actions, &append_op)
                .is_ok()
        );
        assert!(
            checker
                .can_commit(eager, &change_actions, &change_op)
                .is_err()
        );
        assert!(
            checker
                .can_commit(eager, &neutral_actions, &neutral_op)
                .is_ok()
        );

        let actions = create_actions(2, "false", vec![]);
        let snapshot = DeltaTableState::from_actions(actions).await.unwrap();
        let eager = snapshot.snapshot();
        assert!(
            checker
                .can_commit(eager, &append_actions, &append_op)
                .is_ok()
        );
        assert!(
            checker
                .can_commit(eager, &change_actions, &change_op)
                .is_ok()
        );
        assert!(
            checker
                .can_commit(eager, &neutral_actions, &neutral_op)
                .is_ok()
        );

        let actions = create_actions(7, "true", vec![TableFeature::AppendOnly]);
        let snapshot = DeltaTableState::from_actions(actions).await.unwrap();
        let eager = snapshot.snapshot();
        assert!(
            checker
                .can_commit(eager, &append_actions, &append_op)
                .is_ok()
        );
        assert!(
            checker
                .can_commit(eager, &change_actions, &change_op)
                .is_err()
        );
        assert!(
            checker
                .can_commit(eager, &neutral_actions, &neutral_op)
                .is_ok()
        );

        let actions = create_actions(7, "false", vec![TableFeature::AppendOnly]);
        let snapshot = DeltaTableState::from_actions(actions).await.unwrap();
        let eager = snapshot.snapshot();
        assert!(
            checker
                .can_commit(eager, &append_actions, &append_op)
                .is_ok()
        );
        assert!(
            checker
                .can_commit(eager, &change_actions, &change_op)
                .is_ok()
        );
        assert!(
            checker
                .can_commit(eager, &neutral_actions, &neutral_op)
                .is_ok()
        );

        let actions = create_actions(7, "true", vec![]);
        let snapshot = DeltaTableState::from_actions(actions).await.unwrap();
        let eager = snapshot.snapshot();
        assert!(
            checker
                .can_commit(eager, &append_actions, &append_op)
                .is_ok()
        );
        assert!(
            checker
                .can_commit(eager, &change_actions, &change_op)
                .is_ok()
        );
        assert!(
            checker
                .can_commit(eager, &neutral_actions, &neutral_op)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_versions() {
        let checker_1 = ProtocolChecker::new(HashSet::new(), HashSet::new());
        let actions = vec![
            Action::Protocol(
                ProtocolInner {
                    min_reader_version: 1,
                    min_writer_version: 1,
                    ..Default::default()
                }
                .as_kernel(),
            ),
            metadata_action(None).into(),
        ];
        let snapshot_1 = DeltaTableState::from_actions(actions).await.unwrap();
        let eager_1 = snapshot_1.snapshot();
        assert!(checker_1.can_read_from(eager_1).is_ok());
        assert!(checker_1.can_write_to(eager_1).is_ok());

        let checker_2 = ProtocolChecker::new(READER_V2.clone(), HashSet::new());
        let actions = vec![
            Action::Protocol(
                ProtocolInner {
                    min_reader_version: 2,
                    min_writer_version: 1,
                    ..Default::default()
                }
                .as_kernel(),
            ),
            metadata_action(None).into(),
        ];
        let snapshot_2 = DeltaTableState::from_actions(actions).await.unwrap();
        let eager_2 = snapshot_2.snapshot();
        assert!(checker_1.can_read_from(eager_2).is_err());
        assert!(checker_1.can_write_to(eager_2).is_err());
        assert!(checker_2.can_read_from(eager_1).is_ok());
        assert!(checker_2.can_read_from(eager_2).is_ok());
        assert!(checker_2.can_write_to(eager_2).is_ok());

        let checker_3 = ProtocolChecker::new(READER_V2.clone(), WRITER_V2.clone());
        let actions = vec![
            Action::Protocol(
                ProtocolInner {
                    min_reader_version: 2,
                    min_writer_version: 2,
                    ..Default::default()
                }
                .as_kernel(),
            ),
            metadata_action(None).into(),
        ];
        let snapshot_3 = DeltaTableState::from_actions(actions).await.unwrap();
        let eager_3 = snapshot_3.snapshot();
        assert!(checker_1.can_read_from(eager_3).is_err());
        assert!(checker_1.can_write_to(eager_3).is_err());
        assert!(checker_2.can_read_from(eager_3).is_ok());
        assert!(checker_2.can_write_to(eager_3).is_err());
        assert!(checker_3.can_read_from(eager_1).is_ok());
        assert!(checker_3.can_read_from(eager_2).is_ok());
        assert!(checker_3.can_read_from(eager_3).is_ok());
        assert!(checker_3.can_write_to(eager_3).is_ok());

        let checker_4 = ProtocolChecker::new(READER_V2.clone(), WRITER_V3.clone());
        let actions = vec![
            Action::Protocol(
                ProtocolInner {
                    min_reader_version: 2,
                    min_writer_version: 3,
                    ..Default::default()
                }
                .as_kernel(),
            ),
            metadata_action(None).into(),
        ];
        let snapshot_4 = DeltaTableState::from_actions(actions).await.unwrap();
        let eager_4 = snapshot_4.snapshot();
        assert!(checker_1.can_read_from(eager_4).is_err());
        assert!(checker_1.can_write_to(eager_4).is_err());
        assert!(checker_2.can_read_from(eager_4).is_ok());
        assert!(checker_2.can_write_to(eager_4).is_err());
        assert!(checker_3.can_read_from(eager_4).is_ok());
        assert!(checker_3.can_write_to(eager_4).is_err());
        assert!(checker_4.can_read_from(eager_1).is_ok());
        assert!(checker_4.can_read_from(eager_2).is_ok());
        assert!(checker_4.can_read_from(eager_3).is_ok());
        assert!(checker_4.can_read_from(eager_4).is_ok());
        assert!(checker_4.can_write_to(eager_4).is_ok());

        let checker_5 = ProtocolChecker::new(READER_V2.clone(), WRITER_V4.clone());
        let actions = vec![
            Action::Protocol(
                ProtocolInner {
                    min_reader_version: 2,
                    min_writer_version: 4,
                    ..Default::default()
                }
                .as_kernel(),
            ),
            metadata_action(None).into(),
        ];
        let snapshot_5 = DeltaTableState::from_actions(actions).await.unwrap();
        let eager_5 = snapshot_5.snapshot();
        assert!(checker_1.can_read_from(eager_5).is_err());
        assert!(checker_1.can_write_to(eager_5).is_err());
        assert!(checker_2.can_read_from(eager_5).is_ok());
        assert!(checker_2.can_write_to(eager_5).is_err());
        assert!(checker_3.can_read_from(eager_5).is_ok());
        assert!(checker_3.can_write_to(eager_5).is_err());
        assert!(checker_4.can_read_from(eager_5).is_ok());
        assert!(checker_4.can_write_to(eager_5).is_err());
        assert!(checker_5.can_read_from(eager_1).is_ok());
        assert!(checker_5.can_read_from(eager_2).is_ok());
        assert!(checker_5.can_read_from(eager_3).is_ok());
        assert!(checker_5.can_read_from(eager_4).is_ok());
        assert!(checker_5.can_read_from(eager_5).is_ok());
        assert!(checker_5.can_write_to(eager_5).is_ok());

        let checker_6 = ProtocolChecker::new(READER_V2.clone(), WRITER_V5.clone());
        let actions = vec![
            Action::Protocol(
                ProtocolInner {
                    min_reader_version: 2,
                    min_writer_version: 5,
                    ..Default::default()
                }
                .as_kernel(),
            ),
            metadata_action(None).into(),
        ];
        let snapshot_6 = DeltaTableState::from_actions(actions).await.unwrap();
        let eager_6 = snapshot_6.snapshot();
        assert!(checker_1.can_read_from(eager_6).is_err());
        assert!(checker_1.can_write_to(eager_6).is_err());
        assert!(checker_2.can_read_from(eager_6).is_ok());
        assert!(checker_2.can_write_to(eager_6).is_err());
        assert!(checker_3.can_read_from(eager_6).is_ok());
        assert!(checker_3.can_write_to(eager_6).is_err());
        assert!(checker_4.can_read_from(eager_6).is_ok());
        assert!(checker_4.can_write_to(eager_6).is_err());
        assert!(checker_5.can_read_from(eager_6).is_ok());
        assert!(checker_5.can_write_to(eager_6).is_err());
        assert!(checker_6.can_read_from(eager_1).is_ok());
        assert!(checker_6.can_read_from(eager_2).is_ok());
        assert!(checker_6.can_read_from(eager_3).is_ok());
        assert!(checker_6.can_read_from(eager_4).is_ok());
        assert!(checker_6.can_read_from(eager_5).is_ok());
        assert!(checker_6.can_read_from(eager_6).is_ok());
        assert!(checker_6.can_write_to(eager_6).is_ok());

        let checker_7 = ProtocolChecker::new(READER_V2.clone(), WRITER_V6.clone());
        let actions = vec![
            Action::Protocol(
                ProtocolInner {
                    min_reader_version: 2,
                    min_writer_version: 6,
                    ..Default::default()
                }
                .as_kernel(),
            ),
            metadata_action(None).into(),
        ];
        let snapshot_7 = DeltaTableState::from_actions(actions).await.unwrap();
        let eager_7 = snapshot_7.snapshot();
        assert!(checker_1.can_read_from(eager_7).is_err());
        assert!(checker_1.can_write_to(eager_7).is_err());
        assert!(checker_2.can_read_from(eager_7).is_ok());
        assert!(checker_2.can_write_to(eager_7).is_err());
        assert!(checker_3.can_read_from(eager_7).is_ok());
        assert!(checker_3.can_write_to(eager_7).is_err());
        assert!(checker_4.can_read_from(eager_7).is_ok());
        assert!(checker_4.can_write_to(eager_7).is_err());
        assert!(checker_5.can_read_from(eager_7).is_ok());
        assert!(checker_5.can_write_to(eager_7).is_err());
        assert!(checker_6.can_read_from(eager_7).is_ok());
        assert!(checker_6.can_write_to(eager_7).is_err());
        assert!(checker_7.can_read_from(eager_1).is_ok());
        assert!(checker_7.can_read_from(eager_2).is_ok());
        assert!(checker_7.can_read_from(eager_3).is_ok());
        assert!(checker_7.can_read_from(eager_4).is_ok());
        assert!(checker_7.can_read_from(eager_5).is_ok());
        assert!(checker_7.can_read_from(eager_6).is_ok());
        assert!(checker_7.can_read_from(eager_7).is_ok());
        assert!(checker_7.can_write_to(eager_7).is_ok());
    }

    #[tokio::test]
    async fn test_minwriter_v4_with_cdf() {
        let checker_5 = ProtocolChecker::new(READER_V2.clone(), WRITER_V4.clone());
        let actions = vec![
            Action::Protocol(
                ProtocolInner::new(2, 4)
                    .append_writer_features(vec![TableFeature::ChangeDataFeed])
                    .as_kernel(),
            ),
            metadata_action(None).into(),
        ];
        let snapshot_5 = DeltaTableState::from_actions(actions).await.unwrap();
        let eager_5 = snapshot_5.snapshot();
        assert!(checker_5.can_write_to(eager_5).is_ok());
    }

    /// Technically we do not yet support generated columns, but it is okay to "accept" writing to
    /// a column with minWriterVersion=4 and the generated columns feature as long as the
    /// `delta.generationExpression` isn't actually defined the write is still allowed
    #[tokio::test]
    async fn test_minwriter_v4_with_generated_columns() {
        let checker_5 = ProtocolChecker::new(READER_V2.clone(), WRITER_V4.clone());
        let actions = vec![
            Action::Protocol(
                ProtocolInner::new(2, 4)
                    .append_writer_features([TableFeature::GeneratedColumns])
                    .as_kernel(),
            ),
            metadata_action(None).into(),
        ];
        let snapshot_5 = DeltaTableState::from_actions(actions).await.unwrap();
        let eager_5 = snapshot_5.snapshot();
        assert!(checker_5.can_write_to(eager_5).is_ok());
    }

    #[tokio::test]
    async fn test_minwriter_v4_with_generated_columns_and_expressions() {
        let checker_5 = ProtocolChecker::new(Default::default(), WRITER_V4.clone());
        let actions = vec![Action::Protocol(ProtocolInner::new(1, 4).as_kernel())];

        let table = crate::DeltaTable::new_in_memory()
            .create()
            .with_column(
                "value",
                DeltaDataType::Primitive(PrimitiveType::Integer),
                true,
                Some(HashMap::from([(
                    "delta.generationExpression".into(),
                    "x IS TRUE".into(),
                )])),
            )
            .with_actions(actions)
            .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
            .await
            .expect("failed to make a version 4 table with EnableChangeDataFeed");
        let eager_5 = table
            .snapshot()
            .expect("Failed to get snapshot from test table");
        assert!(checker_5.can_write_to(eager_5).is_ok());
    }

    #[tokio::test]
    async fn test_variant_reader_writer_features_are_supported() {
        let checker = ProtocolChecker::new(
            HashSet::from_iter([TableFeature::VariantType, TableFeature::VariantTypePreview]),
            HashSet::from_iter([TableFeature::VariantType, TableFeature::VariantTypePreview]),
        );

        for feature in [TableFeature::VariantType, TableFeature::VariantTypePreview] {
            let actions = vec![
                Action::Protocol(
                    ProtocolInner::new(3, 7)
                        .append_reader_features([feature.clone()])
                        .append_writer_features([feature.clone()])
                        .as_kernel(),
                ),
                metadata_action(None).into(),
            ];
            let snapshot = DeltaTableState::from_actions(actions).await.unwrap();
            let eager = snapshot.snapshot();
            assert!(checker.can_read_from(eager).is_ok());
            assert!(checker.can_write_to(eager).is_ok());
        }
    }

    #[tokio::test]
    async fn test_minreader_v3_checks_explicit_reader_features() {
        let checker = ProtocolChecker::new(
            HashSet::from_iter([TableFeature::VariantType]),
            HashSet::from_iter([TableFeature::VariantType]),
        );

        let actions = vec![
            Action::Protocol(
                ProtocolInner::new(3, 7)
                    .append_reader_features([
                        TableFeature::VariantType,
                        TableFeature::VariantShreddingPreview,
                    ])
                    .append_writer_features([
                        TableFeature::VariantType,
                        TableFeature::VariantShreddingPreview,
                    ])
                    .as_kernel(),
            ),
            metadata_action(None).into(),
        ];
        let snapshot = DeltaTableState::from_actions(actions).await.unwrap();

        let err = checker.can_read_from(snapshot.snapshot()).unwrap_err();
        assert!(matches!(
            err,
            TransactionError::UnsupportedTableFeatures(features)
                if features == vec![TableFeature::VariantShreddingPreview]
        ));
    }

    #[tokio::test]
    async fn test_check_can_write_variant_requires_table_feature() {
        let checker = ProtocolChecker::new(
            HashSet::from_iter([TableFeature::VariantType]),
            HashSet::from_iter([TableFeature::VariantType]),
        );
        let schema = Schema::try_new(vec![StructField::new(
            "v",
            DeltaDataType::unshredded_variant(),
            true,
        )])
        .unwrap();

        let missing_feature = DeltaTableState::from_actions(vec![
            Action::Protocol(
                ProtocolInner::new(3, 7)
                    .append_reader_features([TableFeature::DeletionVectors])
                    .append_writer_features([TableFeature::DeletionVectors])
                    .as_kernel(),
            ),
            metadata_action(None).into(),
        ])
        .await
        .unwrap();
        assert!(
            checker
                .check_can_write_variant(missing_feature.snapshot(), &schema)
                .is_err()
        );

        let preview_feature = DeltaTableState::from_actions(vec![
            Action::Protocol(
                ProtocolInner::new(3, 7)
                    .append_reader_features([TableFeature::VariantTypePreview])
                    .append_writer_features([TableFeature::VariantTypePreview])
                    .as_kernel(),
            ),
            metadata_action(None).into(),
        ])
        .await
        .unwrap();
        assert!(
            checker
                .check_can_write_variant(preview_feature.snapshot(), &schema)
                .is_ok()
        );
    }

    /// An encrypted table whose snapshot `check_encryption_change` can be run against.
    async fn encrypted_table() -> crate::DeltaTable {
        use crate::operations::create::CreateBuilder;

        CreateBuilder::new()
            .with_location("memory:///")
            .with_columns(TestSchemas::simple().fields().cloned())
            .with_configuration_property(TableProperty::EncryptionKmsId, Some("test-kms"))
            .with_configuration_property(TableProperty::EncryptionFooterKey, Some("fk"))
            .with_configuration_property(TableProperty::EncryptionColumnKeys, Some("pii:value"))
            .with_configuration_property(
                TableProperty::EncryptionKmsConfiguration,
                Some(r#"{"endpoint":"a"}"#),
            )
            .await
            .unwrap()
    }

    fn set_properties() -> DeltaOperation {
        DeltaOperation::SetTableProperties {
            properties: HashMap::new(),
        }
    }

    /// Changing the keys of an encrypted table would leave its existing files encrypted
    /// under the old ones, so only the KMS client configuration may change.
    #[tokio::test]
    async fn encryption_keys_cannot_change_on_an_encrypted_table() {
        use crate::kernel::MetadataExt as _;

        let table = encrypted_table().await;
        let snapshot = table.snapshot().unwrap().snapshot();
        let metadata = snapshot.metadata().clone();

        for (key, value) in [
            (TableProperty::EncryptionKmsId, "other-kms"),
            (TableProperty::EncryptionFooterKey, "fk2"),
            (TableProperty::EncryptionPlaintextFooter, "true"),
            (TableProperty::EncryptionColumnKeys, "pii:id"),
        ] {
            let changed = metadata
                .clone()
                .add_config_key(key.as_ref().to_string(), value.to_string())
                .unwrap();
            let err = check_encryption_change(snapshot, &changed, &set_properties())
                .unwrap_err()
                .to_string();
            assert!(err.contains("cannot be changed"), "{}: {err}", key.as_ref());
            assert!(err.contains(key.as_ref()), "{err}");
        }

        // The same configuration, and a different KMS endpoint, are fine.
        check_encryption_change(snapshot, &metadata, &set_properties()).unwrap();
        let reconfigured = metadata
            .add_config_key(
                TableProperty::EncryptionKmsConfiguration
                    .as_ref()
                    .to_string(),
                r#"{"endpoint":"b"}"#.to_string(),
            )
            .unwrap();
        check_encryption_change(snapshot, &reconfigured, &set_properties()).unwrap();
    }

    /// Restore re-installs an earlier version's metadata along with its data files, so
    /// it may remove or change encryption even though other operations may not.
    #[tokio::test]
    async fn restore_may_change_encryption() {
        use crate::kernel::MetadataExt as _;

        let table = encrypted_table().await;
        let snapshot = table.snapshot().unwrap().snapshot();
        let mut plaintext = snapshot.metadata().clone();
        for key in [
            TableProperty::EncryptionKmsId,
            TableProperty::EncryptionFooterKey,
            TableProperty::EncryptionColumnKeys,
            TableProperty::EncryptionKmsConfiguration,
        ] {
            plaintext = plaintext.remove_config_key(key.as_ref()).unwrap();
        }
        let rekeyed = snapshot
            .metadata()
            .clone()
            .add_config_key(
                TableProperty::EncryptionFooterKey.as_ref().to_string(),
                "fk-old".to_string(),
            )
            .unwrap();
        let restore = DeltaOperation::Restore {
            version: Some(0),
            datetime: None,
        };

        let err = check_encryption_change(snapshot, &plaintext, &set_properties())
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot be removed"), "{err}");
        check_encryption_change(snapshot, &plaintext, &restore).unwrap();
        check_encryption_change(snapshot, &rekeyed, &restore).unwrap();
    }
}
