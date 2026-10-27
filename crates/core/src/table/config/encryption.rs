//! Parquet Modular Encryption table properties: parsing, validation and change rules.
use std::collections::HashMap;

#[cfg(all(feature = "datafusion", feature = "encryption"))]
use datafusion::config::{EncryptionFactoryOptions, TableParquetOptions};
use delta_kernel::expressions::ColumnName;
use delta_kernel::schema::{DataType, StructField, StructType};
use delta_kernel::table_features::ColumnMappingMode;

use super::TableProperty;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::arrow::engine_ext::physical_column_name;

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

/// Whether `path` resolves through the schema, each part by `matches(field, name)`.
fn path_exists<'p>(
    schema: &StructType,
    path: impl IntoIterator<Item = &'p str>,
    matches: impl Fn(&StructField, &str) -> bool,
) -> bool {
    let mut fields = Some(schema);
    for part in path {
        let Some(field) = fields.and_then(|s| s.fields().find(|f| matches(f, part))) else {
            return false;
        };
        fields = match field.data_type() {
            DataType::Struct(inner) => Some(inner.as_ref()),
            _ => None,
        };
    }
    true
}

/// Whether a stats column (physical path components) is an encrypted column (RFC
/// dot-separated path) or nested in one. Whole components, so `a.b` is not `a`'s child.
fn stats_column_is_encrypted(physical: &[String], encrypted: &str) -> bool {
    let encrypted: Vec<&str> = encrypted.split('.').collect();
    let physical: Vec<&str> = physical.iter().map(String::as_str).collect();
    physical.starts_with(&encrypted) || encrypted.starts_with(&physical)
}

/// Parquet Modular Encryption settings from a table's `delta.encryption.*` properties, with
/// the names and semantics of the [protocol RFC](https://github.com/delta-io/delta/issues/6195).
///
/// The properties are plaintext in the Delta log, so they hold only key IDs and KMS
/// settings, never keys or credentials. Every build can parse them; encrypting or
/// decrypting needs the `encryption` cargo feature. They are read from the raw metadata
/// `configuration` map, not kernel's `TableProperties`, which keeps only the keys it does
/// not recognise: once kernel types these keys, an encrypted table would look plaintext.
///
/// # Protocol
/// The RFC guards encrypted tables with a `parquetEncryption` reader/writer feature.
/// delta-kernel rejects that feature until it supports it, so delta-rs does not add it yet
/// and recognises encrypted tables by their properties instead. Other engines are not
/// protected until then.
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
    /// Whether any `delta.encryption.*` property is set, valid or not: an invalid
    /// configuration must still not be read or written as plaintext.
    ///
    /// Fail-closed on purpose, like the RFC's `parquetEncryption` feature: treating a
    /// misconfigured table as plaintext would read ciphertext or write plaintext. A stray
    /// key without a footer key, which another engine may leave on a plaintext table,
    /// therefore locks it out of delta-rs, and since the table's configuration is then
    /// invalid, no commit can remove the key: create-or-replace is the way out.
    pub fn is_configured(configuration: &HashMap<String, String>) -> bool {
        configuration
            .keys()
            .any(|key| key.starts_with(ENCRYPTION_PROP_PREFIX))
    }

    /// Refuse a `delta.encryption.*` key this version does not know. Run on the keys a
    /// commit introduces, so a typo is caught before it is committed, but not on read or on
    /// keys already in the table: a property a later RFC revision adds must not lock the
    /// table out of delta-rs, the `parquetEncryption` feature flag is what guards that.
    pub fn check_known_properties<'a>(
        keys: impl IntoIterator<Item = &'a String>,
    ) -> DeltaResult<()> {
        match keys.into_iter().find(|key| {
            key.starts_with(ENCRYPTION_PROP_PREFIX) && !ENCRYPTION_PROPS.contains(&key.as_str())
        }) {
            Some(unknown) => Err(invalid_encryption_config(format!(
                "unknown property '{unknown}'; expected one of {ENCRYPTION_PROPS:?}"
            ))),
            None => Ok(()),
        }
    }

    /// Parse and validate the encryption configuration from a table's raw metadata
    /// `configuration`. `Ok(None)` when no `delta.encryption.*` property is set.
    ///
    /// Per the RFC, `footer_key` turns encryption on, so any other encryption property
    /// without it is an error, as are a missing `kms_id` and malformed values. This is
    /// stricter than the RFC, which lets a table keep `kms_id` with no footer key after
    /// encryption was disabled; delta-rs refuses such a table rather than treat it as
    /// plaintext. Unknown `delta.encryption.*` keys are ignored here; see
    /// [`check_known_properties`](Self::check_known_properties).
    ///
    /// # Why the footer key is required
    /// Parquet always encrypts the footer with it, or signs the footer with it under
    /// `plaintext_footer`; there is no footer-less mode. Column keys refine what the footer
    /// key protects, they never replace it. See the [Parquet specification].
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
    /// A column may appear once, and not alongside an ancestor, which already covers it.
    /// Names are split on dots to address struct fields; the backtick quoting of the
    /// protocol's field-path scheme is not supported, so a column whose name contains a
    /// dot cannot be given its own key (`ColumnName::parse_column_name_list` is the
    /// upgrade path).
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

    /// Check [`column_keys`](Self::column_keys) against the schema: every column must exist
    /// and none may be a partition column. Names are physical and dot-separated.
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
                let by_physical_name = |field: &StructField, name: &str| {
                    field.physical_name(column_mapping_mode) == name
                };
                if !path_exists(schema, column.split('.'), by_physical_name) {
                    return Err(invalid_encryption_config(format!(
                        "key '{key_id}' names column '{column}', which is not in the table"
                    )));
                }
                let top_level = column.split('.').next().unwrap_or_default();
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

    /// Drop from [`column_keys`](Self::column_keys) the columns that are no longer in
    /// `schema`, as the RFC's schema-change rule requires when a column is removed; names
    /// are physical. Removing the last keyed column is an error: an empty `column_keys`
    /// means uniform encryption, which would silently encrypt every column.
    pub fn without_columns_missing_from(
        mut self,
        schema: &StructType,
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<Self> {
        if self.column_keys.is_empty() {
            return Ok(self);
        }
        let by_physical_name =
            |field: &StructField, name: &str| field.physical_name(column_mapping_mode) == name;
        for columns in self.column_keys.values_mut() {
            columns.retain(|column| path_exists(schema, column.split('.'), by_physical_name));
        }
        self.column_keys.retain(|_, columns| !columns.is_empty());
        if self.column_keys.is_empty() {
            return Err(invalid_encryption_config(format!(
                "the schema change removes every column named in \
                 '{ENCRYPTION_COLUMN_KEYS_PROP}', which would switch the table to uniform \
                 encryption; recreate the table instead"
            )));
        }
        Ok(self)
    }

    /// Refuse a `delta.dataSkippingStatsColumns` that names an encrypted column: the Delta
    /// log is plaintext, so the RFC forbids their statistics in it. The writer leaves them
    /// out regardless; this turns the conflict into an error. Stats columns are display
    /// names, resolved to physical names; unknown ones are left to the stats validation.
    /// Partition columns are never in a data file, hence never encrypted.
    pub fn validate_stats_columns(
        &self,
        configuration: &HashMap<String, String>,
        schema: &StructType,
        partition_columns: &[String],
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
            let display = &stats_column;
            if stats_column
                .path()
                .first()
                .is_some_and(|top_level| partition_columns.contains(top_level))
            {
                continue;
            }
            let Some(physical) = physical_column_name(schema, &stats_column, column_mapping_mode)
            else {
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
                .find(|(_, encrypted)| stats_column_is_encrypted(physical.path(), encrypted))
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

    /// The first frozen property that differs between `self` and `other`, or `None` if
    /// both encrypt the same way. Parsed values are compared, so reordering `column_keys`
    /// is not a change. `kms_configuration` is not frozen: it only says how to reach the
    /// KMS.
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

    /// [`changed_frozen_property`](Self::changed_frozen_property) for configurations that
    /// may be absent: turning encryption on or off counts as changing `footer_key`, the
    /// property that turns it on.
    pub fn frozen_change(old: Option<&Self>, new: Option<&Self>) -> Option<&'static str> {
        match (old, new) {
            (None, None) => None,
            (Some(old), Some(new)) => old.changed_frozen_property(new),
            _ => Some(ENCRYPTION_FOOTER_KEY_PROP),
        }
    }

    /// Rewrite [`column_keys`](Self::column_keys) from display names to the physical names
    /// the RFC stores, so renaming a column does not invalidate the configuration. A name
    /// that is already physical is kept, so a stored configuration can be passed back.
    pub fn with_physical_column_names(
        mut self,
        schema: &StructType,
        column_mapping_mode: ColumnMappingMode,
    ) -> DeltaResult<Self> {
        if column_mapping_mode == ColumnMappingMode::None {
            return Ok(self);
        }
        let by_physical_name =
            |field: &StructField, name: &str| field.physical_name(column_mapping_mode) == name;
        for (key_id, columns) in self.column_keys.iter_mut() {
            for column in columns.iter_mut() {
                let logical = ColumnName::new(column.split('.'));
                match physical_column_name(schema, &logical, column_mapping_mode) {
                    Some(physical) => *column = physical.path().join("."),
                    None if path_exists(schema, column.split('.'), by_physical_name) => {}
                    None => {
                        return Err(invalid_encryption_config(format!(
                            "key '{key_id}' names column '{column}', which is not in the table"
                        )));
                    }
                }
            }
        }
        Ok(self)
    }

    /// [`column_keys`](Self::column_keys) in the `delta.encryption.column_keys` format,
    /// sorted so the result is deterministic.
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

    /// The options passed to the registered encryption factory, keyed by the
    /// `delta.encryption.*` property names.
    #[cfg(all(feature = "datafusion", feature = "encryption"))]
    pub fn factory_options(&self) -> EncryptionFactoryOptions {
        let mut opts = EncryptionFactoryOptions::default();
        if let Some(cfg) = &self.kms_configuration {
            opts.options
                .insert(ENCRYPTION_KMS_CONFIGURATION_PROP.to_string(), cfg.clone());
        }
        opts.options.insert(
            ENCRYPTION_FOOTER_KEY_PROP.to_string(),
            self.footer_key.clone(),
        );
        opts.options.insert(
            ENCRYPTION_PLAINTEXT_FOOTER_PROP.to_string(),
            self.plaintext_footer.to_string(),
        );
        if !self.column_keys.is_empty() {
            // Sorted, so factories can use the string as a cache key.
            opts.options.insert(
                ENCRYPTION_COLUMN_KEYS_PROP.to_string(),
                self.column_keys_property(),
            );
        }
        opts
    }
}

#[cfg(test)]
mod tests {
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
        // `footer_key` is the switch (RFC): every other encryption property is an error
        // without it.
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

    /// A misspelt key is caught when introduced; a key a newer engine wrote is tolerated
    /// when the known ones parse.
    #[test]
    fn unknown_encryption_property_is_refused_on_commit_and_ignored_on_parse() {
        let props = props_with(&[
            (ENCRYPTION_KMS_ID_PROP, "kms"),
            (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
            ("delta.encryption.footer.key", "fk"),
        ]);
        let err = EncryptionConfig::check_known_properties(props.keys())
            .unwrap_err()
            .to_string();
        assert!(err.contains("delta.encryption.footer.key"), "{err}");
        let enc = EncryptionConfig::try_from_configuration(&props)
            .unwrap()
            .unwrap();
        assert_eq!(enc.footer_key, "fk");
    }

    /// Dropping a column drops it from `column_keys`; dropping the last keyed column would
    /// mean uniform encryption, so that is refused.
    #[test]
    fn without_columns_missing_from_prunes_dropped_columns() {
        let enc = config_with_column_keys("pii:ssn,address.street,gone;geo:address.gone")
            .without_columns_missing_from(&schema(), ColumnMappingMode::None)
            .unwrap();
        assert_eq!(enc.column_keys_property(), "pii:address.street,ssn");

        let err = config_with_column_keys("pii:gone")
            .without_columns_missing_from(&schema(), ColumnMappingMode::None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("uniform encryption"), "{err}");
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

    /// Reordering, whitespace, an explicit default and a different KMS endpoint are not
    /// changes; each frozen property is reported by name when it is.
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

    /// Stats columns may not name an encrypted column, nor any column under uniform
    /// encryption.
    #[test]
    fn validate_stats_columns_rejects_encrypted_columns() {
        let stats = |cols: &str| props_with(&[("delta.dataSkippingStatsColumns", cols)]);
        let mode = ColumnMappingMode::None;
        let enc = config_with_column_keys("pii:ssn;geo:address.city");
        enc.validate_stats_columns(&props_with(&[]), &schema(), &[], mode)
            .unwrap();
        enc.validate_stats_columns(
            &stats("id, region, address.street, missing"),
            &schema(),
            &[],
            mode,
        )
        .unwrap();
        for cols in ["ssn", "id,ssn", "address.city", "address"] {
            let err = enc
                .validate_stats_columns(&stats(cols), &schema(), &[], mode)
                .unwrap_err()
                .to_string();
            assert!(err.contains("encrypted with key"), "{cols}: {err}");
        }
        let err = config_with_column_keys("")
            .validate_stats_columns(&stats("id"), &schema(), &[], mode)
            .unwrap_err()
            .to_string();
        assert!(err.contains("every column"), "{err}");
        // Partition columns are never in a data file.
        config_with_column_keys("")
            .validate_stats_columns(&stats("region"), &schema(), &["region".to_string()], mode)
            .unwrap();

        // A quoted field whose name contains a dot is one component, not a nested path.
        let dotted = StructType::try_new([
            StructField::nullable("a.b", DataType::STRING),
            StructField::nullable("a", DataType::STRING),
        ])
        .unwrap();
        let err = config_with_column_keys("")
            .validate_stats_columns(&stats("`a.b`"), &dotted, &[], mode)
            .unwrap_err()
            .to_string();
        assert!(err.contains("every column"), "{err}");
        config_with_column_keys("pii:a")
            .validate_stats_columns(&stats("`a.b`"), &dotted, &[], mode)
            .unwrap();
        let err = config_with_column_keys("pii:a")
            .validate_stats_columns(&stats("a"), &dotted, &[], mode)
            .unwrap_err()
            .to_string();
        assert!(err.contains("encrypted with key"), "{err}");
    }

    /// The fixture shows an encrypted table's metadata on disk. Kernel rejects its
    /// `parquetEncryption` feature, so the log is read directly. The configuration is
    /// parsed from the raw metadata map only, which pins the path that survives kernel
    /// learning these keys.
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

    /// Files registered at creation are plaintext, so they cannot be committed as encrypted.
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

        // The stored (physical) names can be passed back.
        let resent = enc
            .clone()
            .with_physical_column_names(&physical_schema, ColumnMappingMode::Name)
            .unwrap();
        assert_eq!(resent.column_keys, enc.column_keys);
        let err = config_with_column_keys("pii:missing")
            .with_physical_column_names(&physical_schema, ColumnMappingMode::Name)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not in the table"), "{err}");
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

    /// Legacy writers and blind appends upload or register files without a commit check,
    /// so they refuse an encrypted table up front.
    #[tokio::test]
    async fn legacy_writers_refuse_encrypted_tables_before_any_upload() {
        use crate::DeltaTableError;
        use crate::kernel::transaction::WRITES_ENCRYPTED_TABLES;
        use crate::table::BlindDeltaTable;
        use crate::writer::{JsonWriter, RecordBatchWriter};

        if WRITES_ENCRYPTED_TABLES {
            return;
        }
        let table = plaintext_table()
            .with_configuration(
                encryption_properties(ENCRYPTION_FOOTER_KEY_PROP)
                    .into_iter()
                    .map(|(k, v)| (k, Some(v))),
            )
            .await
            .unwrap();
        let refused = |err: DeltaTableError| {
            assert!(err.to_string().contains("parquetEncryption"), "{err}");
        };
        refused(RecordBatchWriter::for_table(&table).unwrap_err());
        refused(JsonWriter::for_table(&table).unwrap_err());
        let mut blind = BlindDeltaTable::try_new_with_log_store(table.log_store())
            .await
            .unwrap();
        refused(RecordBatchWriter::for_blind_appends(&blind).unwrap_err());
        refused(blind.commit(vec![]).await.unwrap_err());
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

    /// The encryption check runs against the snapshot a transaction read; a concurrent
    /// replace that turned encryption on makes the conflict checker abort the stale
    /// transaction, so it cannot strip the new configuration.
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

        // Plaintext to plaintext passes the encryption check, but would remove the
        // encryption just committed.
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
