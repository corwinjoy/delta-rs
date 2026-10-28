//! An in-memory KMS for encryption tests. Register [`mock_kms_factory`] with
//! [`register_encryption_factory`](crate::operations::write::encryption::register_encryption_factory)
//! under the tables' `kms_id`; operations with their own DataFusion sessions only look there.
//! Not part of the stable API.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::execution::parquet_encryption::EncryptionFactory;
use parquet_key_management::crypto_factory::CryptoFactory;
use parquet_key_management::errors::{Error, Result};
use parquet_key_management::kms::{KmsClient, KmsClientRef, KmsConnectionInfo};

use crate::operations::write::encryption::KmsEncryptionFactory;

/// A [`KmsClient`] that stores each key under a random token and returns the token. Nothing
/// is encrypted, so a wrapped key is only usable through the client that wrapped it.
#[derive(Debug, Default)]
pub struct InMemoryKmsClient {
    vault: Mutex<Vault>,
}

/// token → (master key ID, key)
type Vault = HashMap<String, (String, Vec<u8>)>;

impl KmsClient for InMemoryKmsClient {
    fn wrap_key(&self, key: &[u8], master_key_id: &str) -> Result<String> {
        let token = uuid::Uuid::new_v4().to_string();
        self.vault
            .lock()
            .unwrap()
            .insert(token.clone(), (master_key_id.to_string(), key.to_vec()));
        Ok(token)
    }

    fn unwrap_key(&self, wrapped_key: &str, master_key_id: &str) -> Result<Vec<u8>> {
        let vault = self.vault.lock().unwrap();
        let (wrapped_under, key) = vault.get(wrapped_key).ok_or_else(|| {
            Error::General(format!(
                "in-memory KMS: unknown wrapped key for master key '{master_key_id}'"
            ))
        })?;
        if wrapped_under != master_key_id {
            return Err(Error::General(format!(
                "in-memory KMS: key was wrapped under '{wrapped_under}', not '{master_key_id}'"
            )));
        }
        Ok(key.clone())
    }
}

/// The reference [`KmsEncryptionFactory`] over a fresh [`InMemoryKmsClient`].
pub fn mock_kms_factory() -> Arc<dyn EncryptionFactory> {
    let client: KmsClientRef = Arc::new(InMemoryKmsClient::default());
    Arc::new(KmsEncryptionFactory::new(move || {
        let client = Arc::clone(&client);
        CryptoFactory::new(move |_: &KmsConnectionInfo| Ok(Arc::clone(&client)))
    }))
}
