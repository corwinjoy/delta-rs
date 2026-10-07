//! The reference [`EncryptionFactory`]: envelope encryption with a pluggable KMS.
//!
//! Each Parquet file gets its own random data encryption keys (DEKs): one for the footer
//! and one per column key. Every DEK is wrapped (encrypted) by the KMS under the master key
//! ID the table properties name, and the wrapped DEK is stored in the file's own Parquet
//! key metadata as a key material JSON document. Readers unwrap it through the same KMS.
//! The KMS therefore only ever wraps and unwraps DEKs; it never sees the data.
//!
//! # What a reader depends on
//! Decryption needs only the KMS named by `delta.encryption.kms_id` (via
//! `delta.encryption.kms_configuration`, if the KMS client reads it) and the key metadata
//! in the file. The table's current `footer_key` and `column_keys` play no part, as the
//! Delta protocol RFC requires: they describe how new files are written and may have
//! changed since a file was written. [`KmsEncryptionFactory::get_file_decryption_properties`]
//! does not read them even when a caller passes them.
//!
//! # Key material format
//! The key metadata follows the key material JSON of the parquet-mr and PyArrow key
//! toolkit (`"keyMaterialType": "PKMT1"`), stored internally in the file, so files written
//! by delta-rs can be read by those engines through an equivalent KMS client, and the other
//! way round. [`KEY_MATERIAL_TYPE`] is the version tag: a reader that meets another type
//! refuses the file rather than guessing.
//!
//! # Additional authenticated data
//! The file name is the AAD prefix (see [`WriterPropertiesFactory`]) and is not stored in
//! the file: readers derive it from the path they are given, which is relative to the table
//! root on both sides.
//!
//! [`WriterPropertiesFactory`]: crate::writer::writer_factory::WriterPropertiesFactory

use std::fmt::Debug;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use datafusion::config::EncryptionFactoryOptions;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::parquet_encryption::EncryptionFactory;
use object_store::path::Path;
use parquet::encryption::decrypt::{FileDecryptionProperties, KeyRetriever};
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::errors::ParquetError;
use rand::Rng as _;
use serde::{Deserialize, Serialize};

use crate::errors::{DeltaResult, DeltaTableError};
use crate::table::config::{
    ENCRYPTION_COLUMN_KEYS_PROP, ENCRYPTION_FOOTER_KEY_PROP, ENCRYPTION_KMS_ID_PROP,
    ENCRYPTION_PLAINTEXT_FOOTER_PROP, EncryptionConfig,
};

/// The key material format this factory writes and reads.
pub const KEY_MATERIAL_TYPE: &str = "PKMT1";

/// The length in bytes of the data encryption keys this factory generates (AES-128).
pub const DATA_KEY_LENGTH: usize = 16;

/// A key management system that wraps and unwraps data encryption keys with master keys.
///
/// This is the extension point for encryption: implement it for your KMS and hand it to
/// [`KmsEncryptionFactory`]. A master key never leaves the KMS; `wrap_key` returns an
/// opaque blob that only `unwrap_key` on the same KMS can turn back into the key.
///
/// The methods are synchronous because Parquet asks for keys from a synchronous
/// [`KeyRetriever`] while decoding a footer. A client that talks to a remote service should
/// do so with a blocking client and cache unwrapped keys where its security policy allows.
pub trait KmsClient: Send + Sync + Debug {
    /// Encrypt `key` under the master key `master_key_id`.
    fn wrap_key(&self, key: &[u8], master_key_id: &str) -> DeltaResult<Vec<u8>>;

    /// Decrypt a key that [`wrap_key`](Self::wrap_key) encrypted under `master_key_id`.
    fn unwrap_key(&self, wrapped_key: &[u8], master_key_id: &str) -> DeltaResult<Vec<u8>>;
}

/// The key material stored in a file's key metadata, one document per key.
///
/// Field names follow the parquet-mr key toolkit so the files interoperate; see the module
/// documentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyMaterial {
    /// Always [`KEY_MATERIAL_TYPE`].
    pub key_material_type: String,
    /// Always `true`: the material is stored in the file, not in a separate key store.
    pub internal_storage: bool,
    /// Whether this is the footer key (`true`) or a column key (`false`).
    pub is_footer_key: bool,
    /// The table's `delta.encryption.kms_id`, written with the footer key only.
    #[serde(
        default,
        rename = "kmsInstanceID",
        skip_serializing_if = "Option::is_none"
    )]
    pub kms_instance_id: Option<String>,
    /// Reserved for parquet-mr compatibility; delta-rs writes `"DEFAULT"` with the footer
    /// key and does not use it.
    #[serde(
        default,
        rename = "kmsInstanceURL",
        skip_serializing_if = "Option::is_none"
    )]
    pub kms_instance_url: Option<String>,
    /// The master key ID from the table properties that wrapped the data key.
    #[serde(rename = "masterKeyID")]
    pub master_key_id: String,
    /// The data encryption key, wrapped by the KMS and base64-encoded.
    #[serde(rename = "wrappedDEK")]
    pub wrapped_dek: String,
    /// Always `false`: the data key is wrapped directly with the master key.
    pub double_wrapping: bool,
}

impl KeyMaterial {
    fn to_bytes(&self) -> DeltaResult<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| {
            DeltaTableError::Generic(format!("failed to serialise encryption key material: {e}"))
        })
    }

    /// Parse key metadata written by [`KmsEncryptionFactory`], refusing other formats.
    pub fn from_bytes(key_metadata: &[u8]) -> DeltaResult<Self> {
        let material: Self = serde_json::from_slice(key_metadata).map_err(|e| {
            DeltaTableError::Generic(format!(
                "the Parquet key metadata is not key material delta-rs understands: {e}"
            ))
        })?;
        if material.key_material_type != KEY_MATERIAL_TYPE {
            return Err(DeltaTableError::Generic(format!(
                "unsupported encryption key material type '{}'; this build reads '{}'",
                material.key_material_type, KEY_MATERIAL_TYPE
            )));
        }
        if !material.internal_storage {
            return Err(DeltaTableError::Generic(
                "encryption key material stored outside the Parquet file is not supported"
                    .to_string(),
            ));
        }
        Ok(material)
    }
}

/// The reference [`EncryptionFactory`]: per-file data keys wrapped by a [`KmsClient`], with
/// the wrapped keys stored in each file. See the module documentation.
#[derive(Debug)]
pub struct KmsEncryptionFactory {
    kms: Arc<dyn KmsClient>,
}

impl KmsEncryptionFactory {
    /// A factory that wraps and unwraps data keys with `kms`.
    pub fn new(kms: Arc<dyn KmsClient>) -> Self {
        Self { kms }
    }

    /// The KMS client this factory uses.
    pub fn kms(&self) -> &Arc<dyn KmsClient> {
        &self.kms
    }

    fn wrapped_data_key(
        &self,
        master_key_id: &str,
        is_footer_key: bool,
        kms_instance_id: &str,
    ) -> DeltaResult<(Vec<u8>, Vec<u8>)> {
        let mut key = vec![0u8; DATA_KEY_LENGTH];
        rand::rng().fill_bytes(&mut key);
        let wrapped = self.kms.wrap_key(&key, master_key_id)?;
        let material = KeyMaterial {
            key_material_type: KEY_MATERIAL_TYPE.to_string(),
            internal_storage: true,
            is_footer_key,
            kms_instance_id: is_footer_key.then(|| kms_instance_id.to_string()),
            kms_instance_url: is_footer_key.then(|| "DEFAULT".to_string()),
            master_key_id: master_key_id.to_string(),
            wrapped_dek: BASE64.encode(wrapped),
            double_wrapping: false,
        };
        Ok((key, material.to_bytes()?))
    }
}

/// The file name of `file_path`: the AAD prefix on both the write and the read side.
fn aad_prefix(file_path: &Path) -> Vec<u8> {
    file_path
        .filename()
        .unwrap_or(file_path.as_ref())
        .as_bytes()
        .to_vec()
}

fn external(err: DeltaTableError) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

#[async_trait]
impl EncryptionFactory for KmsEncryptionFactory {
    async fn get_file_encryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        _schema: &Arc<ArrowSchema>,
        file_path: &Path,
    ) -> DataFusionResult<Option<Arc<FileEncryptionProperties>>> {
        let option = |key: &str| config.options.get(key).map(String::as_str);
        let footer_key_id = option(ENCRYPTION_FOOTER_KEY_PROP).ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "encryption factory option '{ENCRYPTION_FOOTER_KEY_PROP}' is required to encrypt"
            ))
        })?;
        let kms_id = option(ENCRYPTION_KMS_ID_PROP).unwrap_or_default();
        let plaintext_footer = match option(ENCRYPTION_PLAINTEXT_FOOTER_PROP) {
            None => false,
            Some(value) => value.parse::<bool>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "encryption factory option '{ENCRYPTION_PLAINTEXT_FOOTER_PROP}' must be a \
                     boolean, got '{value}'"
                ))
            })?,
        };
        let column_keys = EncryptionConfig::parse_column_keys(option(ENCRYPTION_COLUMN_KEYS_PROP))
            .map_err(external)?;

        let (footer_key, footer_metadata) = self
            .wrapped_data_key(footer_key_id, true, kms_id)
            .map_err(external)?;
        let mut builder = FileEncryptionProperties::builder(footer_key)
            .with_footer_key_metadata(footer_metadata)
            .with_plaintext_footer(plaintext_footer)
            .with_aad_prefix(aad_prefix(file_path))
            .with_aad_prefix_storage(false);
        for (master_key_id, columns) in column_keys {
            for column in columns {
                let (key, metadata) = self
                    .wrapped_data_key(&master_key_id, false, kms_id)
                    .map_err(external)?;
                builder = builder.with_column_key_and_metadata(&column, key, metadata);
            }
        }
        Ok(Some(builder.build()?))
    }

    /// Decryption keys for `file_path`, from its own key metadata and the KMS. The options
    /// are not read: see the module documentation.
    async fn get_file_decryption_properties(
        &self,
        _config: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> DataFusionResult<Option<Arc<FileDecryptionProperties>>> {
        let retriever = Arc::new(KmsKeyRetriever {
            kms: Arc::clone(&self.kms),
        });
        Ok(Some(
            FileDecryptionProperties::with_key_retriever(retriever)
                .with_aad_prefix(aad_prefix(file_path))
                .build()?,
        ))
    }
}

/// Unwraps the data key named by a file's key material through the KMS.
#[derive(Debug)]
struct KmsKeyRetriever {
    kms: Arc<dyn KmsClient>,
}

impl KeyRetriever for KmsKeyRetriever {
    fn retrieve_key(&self, key_metadata: &[u8]) -> parquet::errors::Result<Vec<u8>> {
        let general = |err: DeltaTableError| ParquetError::General(err.to_string());
        let material = KeyMaterial::from_bytes(key_metadata).map_err(general)?;
        let wrapped = BASE64.decode(&material.wrapped_dek).map_err(|e| {
            ParquetError::General(format!(
                "encryption key material has a malformed wrappedDEK: {e}"
            ))
        })?;
        self.kms
            .unwrap_key(&wrapped, &material.master_key_id)
            .map_err(general)
    }
}

/// Resolve the key metadata of every key in a file's encryption properties, for tests and
/// diagnostics: the footer key material, then the column key materials by column name.
#[cfg(test)]
pub(crate) fn key_materials(
    properties: &FileEncryptionProperties,
) -> DeltaResult<(KeyMaterial, std::collections::HashMap<String, KeyMaterial>)> {
    let footer = KeyMaterial::from_bytes(
        properties
            .footer_key_metadata()
            .ok_or_else(|| DeltaTableError::Generic("no footer key metadata".to_string()))?,
    )?;
    let mut columns = std::collections::HashMap::new();
    let (names, _keys, metadatas) = properties.column_keys();
    for (name, metadata) in names.into_iter().zip(metadatas) {
        columns.insert(name, KeyMaterial::from_bytes(&metadata)?);
    }
    Ok((footer, columns))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::test_utils::kms_encryption::InMemoryKmsClient;

    fn factory() -> (KmsEncryptionFactory, Arc<InMemoryKmsClient>) {
        let kms = Arc::new(InMemoryKmsClient::default());
        (KmsEncryptionFactory::new(kms.clone()), kms)
    }

    fn options(entries: &[(&str, &str)]) -> EncryptionFactoryOptions {
        let mut options = EncryptionFactoryOptions::default();
        for (key, value) in entries {
            options.options.insert(key.to_string(), value.to_string());
        }
        options
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

    async fn encrypt(factory: &KmsEncryptionFactory, options: &EncryptionFactoryOptions) -> Bytes {
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
        factory: &KmsEncryptionFactory,
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

    /// Each key gets its own wrapped data key, stored in the file as PKMT1 key material.
    #[tokio::test]
    async fn key_material_names_the_master_keys() {
        let (factory, _) = factory();
        let properties = factory
            .get_file_encryption_properties(
                &options(&[
                    (ENCRYPTION_KMS_ID_PROP, "my-kms"),
                    (ENCRYPTION_FOOTER_KEY_PROP, "footer-master"),
                    (ENCRYPTION_COLUMN_KEYS_PROP, "pii-master:ssn"),
                ]),
                &batch().schema(),
                &Path::from("part-0.parquet"),
            )
            .await
            .unwrap()
            .unwrap();
        let (footer, columns) = key_materials(&properties).unwrap();
        assert_eq!(footer.key_material_type, KEY_MATERIAL_TYPE);
        assert!(footer.is_footer_key);
        assert_eq!(footer.master_key_id, "footer-master");
        assert_eq!(footer.kms_instance_id.as_deref(), Some("my-kms"));
        assert!(footer.internal_storage && !footer.double_wrapping);
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
    }

    /// Decryption uses only the file's key metadata and the KMS: the current table
    /// properties, here a rotated footer key and no column keys at all, do not matter.
    #[tokio::test]
    async fn decrypts_from_file_key_metadata_regardless_of_options() {
        let (factory, _) = factory();
        let file = encrypt(
            &factory,
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
            let batch = decrypt(&factory, &current, file.clone()).await.unwrap();
            assert_eq!(batch.num_rows(), 2);
        }
    }

    /// A KMS that cannot unwrap the key refuses the file instead of returning garbage.
    #[tokio::test]
    async fn unknown_wrapped_key_is_refused() {
        let (writer_factory, _) = factory();
        let file = encrypt(
            &writer_factory,
            &options(&[
                (ENCRYPTION_KMS_ID_PROP, "a"),
                (ENCRYPTION_FOOTER_KEY_PROP, "fk"),
            ]),
        )
        .await;
        let (other_factory, _) = factory();
        let err = decrypt(&other_factory, &options(&[]), file)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unknown wrapped key"), "{err}");
    }

    #[test]
    fn foreign_key_material_is_refused() {
        let err = KeyMaterial::from_bytes(br#"{"keyMaterialType":"PKMT2","internalStorage":true,"isFooterKey":true,"masterKeyID":"k","wrappedDEK":"","doubleWrapping":false}"#).unwrap_err();
        assert!(err.to_string().contains("PKMT2"), "{err}");
        assert!(KeyMaterial::from_bytes(b"not json").is_err());
    }
}
