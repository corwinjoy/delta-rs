//! The reference [`EncryptionFactory`], built on [`parquet_key_management`]: the Rust
//! implementation of the Parquet Key Management Tools (PKMT) that parquet-java and PyArrow
//! share. Each file gets random data keys, wrapped through a
//! [`KmsClient`](parquet_key_management::kms::KmsClient) under the table's master key IDs
//! and stored in the file's key metadata as `PKMT1` key material; readers unwrap them
//! through the same KMS, which never sees the data. The crate's own parquet and DataFusion
//! integrations are not used, so delta-rs can follow arrow and parquet releases.
//!
//! Decryption uses only `kms_id`, `kms_configuration` and the file's key metadata, never
//! the table's current `footer_key` or `column_keys`, as the RFC requires.
//!
//! `kms_id` is the KMS instance ID (`kmsInstanceID` in the footer key material).
//! `kms_configuration` reaches the client factory as the custom option named
//! [`ENCRYPTION_KMS_CONFIGURATION_PROP`]; the crate's client cache ignores custom options,
//! so the factory keeps one [`CryptoFactory`] per distinct `kms_configuration`.
//!
//! It is plaintext in the Delta log and anyone who can commit to the table can change it, so a
//! client factory must treat it as untrusted: a client that takes its endpoint from it should
//! accept only allowlisted endpoints, or key requests and credentials go wherever the table says.
//!
//! The file name is the AAD prefix, stored in the file so any reader uses it untold;
//! decryption supplies none, so files written without one read too.

use std::fmt::Debug;
use std::sync::Arc;

use dashmap::DashMap;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::config::EncryptionFactoryOptions;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::parquet_encryption::EncryptionFactory;
use object_store::path::Path;
use parquet::encryption::decrypt::{FileDecryptionProperties, KeyRetriever};
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::errors::ParquetError;
use parquet_key_management::crypto_factory::{
    CryptoFactory, DecryptionConfiguration, EncryptionConfiguration,
};
use parquet_key_management::key_unwrapper::KeyUnwrapper;
use parquet_key_management::kms::KmsConnectionConfig;

use crate::kernel::spawn_blocking_with_span;
use crate::table::config::{
    ENCRYPTION_FOOTER_KEY_PROP, ENCRYPTION_KMS_CONFIGURATION_PROP, ENCRYPTION_KMS_ID_PROP,
    EncryptionConfig,
};

/// The reference [`EncryptionFactory`]; see the module documentation.
pub struct KmsEncryptionFactory {
    make_crypto: Box<dyn Fn() -> CryptoFactory + Send + Sync>,
    // ponytail: one CryptoFactory per `kms_configuration` because the crate's client cache
    // ignores custom options; collapses to one once parquet-key-management keys on them.
    /// `kms_configuration` (`""` when unset) → its crypto factory.
    by_configuration: DashMap<String, Arc<CryptoFactory>>,
}

impl KmsEncryptionFactory {
    /// `make_crypto` builds a [`CryptoFactory`] per distinct `kms_configuration`, e.g.
    /// `|| CryptoFactory::new(my_kms_client_factory.clone())`.
    pub fn new(make_crypto: impl Fn() -> CryptoFactory + Send + Sync + 'static) -> Self {
        Self {
            make_crypto: Box::new(make_crypto),
            by_configuration: DashMap::new(),
        }
    }

    fn crypto(&self, kms_configuration: Option<&str>) -> Arc<CryptoFactory> {
        let key = kms_configuration.unwrap_or_default();
        if let Some(crypto) = self.by_configuration.get(key) {
            return Arc::clone(&crypto);
        }
        Arc::clone(
            &self
                .by_configuration
                .entry(key.to_string())
                .or_insert_with(|| Arc::new((self.make_crypto)())),
        )
    }
}

impl Debug for KmsEncryptionFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsEncryptionFactory")
            .finish_non_exhaustive()
    }
}

fn external(err: parquet_key_management::errors::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

/// The KMS connection for `kms_id` and the table's `kms_configuration`.
fn connection(kms_id: &str, kms_configuration: Option<&str>) -> Arc<KmsConnectionConfig> {
    let mut builder = KmsConnectionConfig::builder().set_kms_instance_id(kms_id.to_string());
    if let Some(cfg) = kms_configuration {
        builder = builder.set_custom_kms_conf_option(
            ENCRYPTION_KMS_CONFIGURATION_PROP.to_string(),
            cfg.to_string(),
        );
    }
    Arc::new(builder.build())
}

#[async_trait]
impl EncryptionFactory for KmsEncryptionFactory {
    async fn get_file_encryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        _schema: &Arc<ArrowSchema>,
        file_path: &Path,
    ) -> DataFusionResult<Option<Arc<FileEncryptionProperties>>> {
        // The table's properties, `column_keys` resolved to leaf paths by the layer.
        let enc = EncryptionConfig::try_from_configuration(&config.options)?.ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "encryption factory option '{ENCRYPTION_FOOTER_KEY_PROP}' is required to encrypt"
            ))
        })?;
        let mut encryption = EncryptionConfiguration::builder(enc.footer_key.clone())
            .set_plaintext_footer(enc.plaintext_footer);
        for key in &enc.column_keys {
            let leaves = key
                .columns
                .iter()
                .map(|leaf| leaf.path().join("."))
                .collect();
            encryption = encryption.add_column_key(key.key_id.clone(), leaves);
        }
        let encryption = encryption.build().map_err(external)?;
        let connection = connection(&enc.kms_id, enc.kms_configuration.as_deref());
        // The KMS client is synchronous, so its round trips run off the async executor.
        let crypto = self.crypto(enc.kms_configuration.as_deref());
        let keys =
            spawn_blocking_with_span(move || crypto.file_encryption_keys(connection, &encryption))
                .await
                .map_err(|err| DataFusionError::External(Box::new(err)))?
                .map_err(external)?;

        let footer = keys.footer_key();
        let mut builder = FileEncryptionProperties::builder(footer.key().to_vec())
            .with_footer_key_metadata(footer.metadata().to_vec())
            .with_plaintext_footer(keys.plaintext_footer())
            // The AAD prefix is the file name.
            .with_aad_prefix(
                file_path
                    .filename()
                    .unwrap_or(file_path.as_ref())
                    .as_bytes()
                    .to_vec(),
            )
            .with_aad_prefix_storage(true);
        for (leaf, key) in keys.column_keys() {
            builder = builder.with_column_key_and_metadata(
                leaf,
                key.key().to_vec(),
                key.metadata().to_vec(),
            );
        }
        Ok(Some(builder.build()?))
    }

    /// Decryption keys from the file's key metadata; reads only `kms_id` and
    /// `kms_configuration`.
    async fn get_file_decryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        _file_path: &Path,
    ) -> DataFusionResult<Option<Arc<FileDecryptionProperties>>> {
        let kms_configuration = config
            .options
            .get(ENCRYPTION_KMS_CONFIGURATION_PROP)
            .map(String::as_str);
        let connection = connection(
            config
                .options
                .get(ENCRYPTION_KMS_ID_PROP)
                .map(String::as_str)
                .unwrap_or_default(),
            kms_configuration,
        );
        let unwrapper = self
            .crypto(kms_configuration)
            .key_unwrapper(connection, DecryptionConfiguration::default())
            .map_err(external)?;
        Ok(Some(
            FileDecryptionProperties::with_key_retriever(Arc::new(KmsKeyRetriever(unwrapper)))
                .build()?,
        ))
    }
}

/// Parquet's [`KeyRetriever`] over the crate's [`KeyUnwrapper`].
struct KmsKeyRetriever(KeyUnwrapper);

impl KeyRetriever for KmsKeyRetriever {
    fn retrieve_key(&self, key_metadata: &[u8]) -> parquet::errors::Result<Vec<u8>> {
        self.0.unwrap_key(key_metadata).map_err(|err| {
            ParquetError::General(format!(
                "could not unwrap a data key through the KMS: {err}. The KMS is the one \
                 registered as delta.encryption.kms_id, configured by \
                 delta.encryption.kms_configuration; check both if the KMS has moved"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    use parquet::file::properties::WriterProperties;
    use parquet_key_management::kms::{KmsClientRef, KmsConnectionInfo};

    use super::*;
    use crate::table::config::{ENCRYPTION_COLUMN_KEYS_PROP, ENCRYPTION_KMS_ID_PROP};
    use crate::test_utils::kms_encryption::{InMemoryKmsClient, mock_kms_factory};
    use parquet_key_management::key_material::KeyMaterial;

    /// The footer key material and the column key materials by column name.
    fn key_materials(
        properties: &FileEncryptionProperties,
    ) -> (KeyMaterial, std::collections::HashMap<String, KeyMaterial>) {
        let parse =
            |bytes: &[u8]| KeyMaterial::deserialize(std::str::from_utf8(bytes).unwrap()).unwrap();
        let footer = parse(properties.footer_key_metadata().unwrap());
        let (names, _keys, metadatas) = properties.column_keys();
        let columns = names
            .into_iter()
            .zip(metadatas)
            .map(|(name, metadata)| (name, parse(&metadata)))
            .collect();
        (footer, columns)
    }

    fn options(entries: &[(&str, &str)]) -> EncryptionFactoryOptions {
        EncryptionFactoryOptions {
            options: entries
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("ssn", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap()
    }

    async fn encrypt(factory: &dyn EncryptionFactory, options: &EncryptionFactoryOptions) -> Bytes {
        let batch = batch();
        let path = Path::from("part-0.parquet");
        let encryption = factory
            .get_file_encryption_properties(options, &batch.schema(), &path)
            .await
            .unwrap()
            .unwrap();
        let properties = WriterProperties::builder()
            .with_file_encryption_properties(encryption)
            .build();
        let mut buffer = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(buffer)
    }

    async fn decrypt(
        factory: &dyn EncryptionFactory,
        options: &EncryptionFactoryOptions,
        file: Bytes,
    ) -> parquet::errors::Result<RecordBatch> {
        let decryption = factory
            .get_file_decryption_properties(options, &Path::from("part-0.parquet"))
            .await
            .unwrap()
            .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
            file,
            ArrowReaderOptions::new().with_file_decryption_properties(decryption),
        )?
        .build()?;
        reader
            .into_iter()
            .next()
            .unwrap()
            .map_err(|e| ParquetError::ArrowError(e.to_string()))
    }

    /// `kms_id` and `kms_configuration` reach the client factory, and each configuration
    /// gets its own client.
    #[tokio::test]
    async fn kms_configuration_reaches_the_client_factory_and_isolates_clients() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let client: KmsClientRef = Arc::new(InMemoryKmsClient::default());
        let factory = KmsEncryptionFactory::new({
            let seen = Arc::clone(&seen);
            move || {
                let seen = Arc::clone(&seen);
                let client = Arc::clone(&client);
                CryptoFactory::new(move |info: &KmsConnectionInfo| {
                    seen.lock().unwrap().push((
                        info.kms_instance_id().to_string(),
                        info.custom_kms_conf()
                            .get(ENCRYPTION_KMS_CONFIGURATION_PROP)
                            .cloned(),
                    ));
                    Ok(Arc::clone(&client))
                })
            }
        });
        for vault in ["vault-a", "vault-b"] {
            let file = encrypt(
                &factory,
                &options(&[
                    (ENCRYPTION_KMS_ID_PROP, "kms"),
                    (ENCRYPTION_KMS_CONFIGURATION_PROP, vault),
                    (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
                ]),
            )
            .await;
            decrypt(
                &factory,
                &options(&[
                    (ENCRYPTION_KMS_ID_PROP, "kms"),
                    (ENCRYPTION_KMS_CONFIGURATION_PROP, vault),
                ]),
                file,
            )
            .await
            .unwrap();
        }
        let seen = seen.lock().unwrap();
        assert!(seen.iter().all(|(id, _)| id == "kms"), "{seen:?}");
        let vaults: std::collections::HashSet<_> =
            seen.iter().map(|(_, cfg)| cfg.clone()).collect();
        assert_eq!(
            vaults,
            [Some("vault-a".to_string()), Some("vault-b".to_string())].into(),
            "{seen:?}"
        );
    }

    /// Each key gets its own wrapped data key, stored in the file as PKMT1 key material.
    #[tokio::test]
    async fn key_material_names_the_master_keys() {
        let factory = mock_kms_factory();
        let properties = factory
            .get_file_encryption_properties(
                &options(&[
                    (ENCRYPTION_KMS_ID_PROP, "my-kms"),
                    (ENCRYPTION_FOOTER_KEY_PROP, "footer-master"),
                    // Leaf paths, as the encryption layer passes them.
                    (ENCRYPTION_COLUMN_KEYS_PROP, "pii-master:ssn"),
                ]),
                &batch().schema(),
                &Path::from("part-0.parquet"),
            )
            .await
            .unwrap()
            .unwrap();
        let (footer, columns) = key_materials(&properties);
        assert!(footer.is_footer_key);
        assert_eq!(footer.master_key_id, "footer-master");
        assert_eq!(footer.kms_instance_id.as_deref(), Some("my-kms"));
        assert!(footer.internal_storage && footer.double_wrapping);
        let ssn = &columns["ssn"];
        assert!(!ssn.is_footer_key);
        assert_eq!(ssn.master_key_id, "pii-master");
        assert!(ssn.kms_instance_id.is_none());
        assert_ne!(ssn.wrapped_dek, footer.wrapped_dek);

        let json: serde_json::Value =
            serde_json::from_slice(properties.footer_key_metadata().unwrap()).unwrap();
        assert_eq!(json["keyMaterialType"], "PKMT1");
        assert_eq!(json["masterKeyID"], "footer-master");
        assert_eq!(json["kmsInstanceID"], "my-kms");
        assert!(json["wrappedDEK"].is_string());

        // The AAD prefix is the file name and is stored in the file for other readers.
        assert_eq!(properties.aad_prefix(), Some(&b"part-0.parquet".to_vec()));
        assert!(properties.store_aad_prefix());
    }

    /// Decryption uses only the file's key metadata and the KMS: the current table
    /// properties, here a rotated footer key and no column keys at all, do not matter.
    #[tokio::test]
    async fn decrypts_from_file_key_metadata_regardless_of_options() {
        let factory = mock_kms_factory();
        let file = encrypt(
            factory.as_ref(),
            &options(&[
                (ENCRYPTION_KMS_ID_PROP, "my-kms"),
                (ENCRYPTION_FOOTER_KEY_PROP, "footer-v1"),
                (ENCRYPTION_COLUMN_KEYS_PROP, "pii-v1:ssn"),
            ]),
        )
        .await;
        for current in [
            options(&[(ENCRYPTION_KMS_ID_PROP, "my-kms")]),
            options(&[
                (ENCRYPTION_KMS_ID_PROP, "my-kms"),
                (ENCRYPTION_FOOTER_KEY_PROP, "footer-v2"),
            ]),
        ] {
            let batch = decrypt(factory.as_ref(), &current, file.clone())
                .await
                .unwrap();
            assert_eq!(batch.num_rows(), 2);
        }
    }

    /// A KMS that cannot unwrap the key refuses the file instead of returning garbage.
    #[tokio::test]
    async fn unknown_wrapped_key_is_refused() {
        let file = encrypt(
            mock_kms_factory().as_ref(),
            &options(&[
                (ENCRYPTION_KMS_ID_PROP, "a"),
                (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
            ]),
        )
        .await;
        let err = decrypt(mock_kms_factory().as_ref(), &options(&[]), file)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unknown wrapped key"), "{err}");
    }
}
