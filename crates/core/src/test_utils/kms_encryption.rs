//! An in-memory KMS for testing encryption via `delta.encryption.*` table properties.
//!
//! This module is **not part of the stable public API**. It lives in `test_utils`, which is
//! compiled for this crate's tests and, with the `integration_test` feature, for downstream
//! crates' integration tests. Do not rely on it for production use.
//!
//! # Usage
//!
//! 1. Register [`mock_kms_factory`] in the process-wide registry with
//!    [`register_encryption_factory`](crate::operations::write::encryption::register_encryption_factory).
//!    Operations that create their own DataFusion sessions, such as `table.write()`, only
//!    find factories registered there.
//! 2. Create a Delta table with `delta.encryption.kms_id` set to the same ID.
//! 3. All subsequent read/write operations on the table will use the registered factory.
//!
//! ```rust,ignore
//! // Register factory at startup
//! register_encryption_factory("test-kms", mock_kms_factory());
//!
//! // Create encrypted table
//! table.create()
//!     .with_property("delta.encryption.kms_id", "test-kms")
//!     .with_property("delta.encryption.footer_key", "my-key")
//!     .await?;
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::execution::parquet_encryption::EncryptionFactory;

use crate::errors::{DeltaResult, DeltaTableError};
use crate::operations::write::encryption::{KmsClient, KmsEncryptionFactory};

/// A [`KmsClient`] that keeps every wrapped key in memory, like a vault that stores keys
/// and hands out tokens.
///
/// `wrap_key` stores the data key under a random token and returns the token;
/// `unwrap_key` looks the token up and checks it was wrapped under the same master key.
/// Nothing is encrypted, so a wrapped key is only usable in the process that wrapped it,
/// which is what tests need: a file written by one factory cannot be read through another.
#[derive(Debug, Default)]
pub struct InMemoryKmsClient {
    vault: Mutex<Vault>,
}

/// token → (master key ID, data key)
type Vault = HashMap<Vec<u8>, (String, Vec<u8>)>;

impl KmsClient for InMemoryKmsClient {
    fn wrap_key(&self, key: &[u8], master_key_id: &str) -> DeltaResult<Vec<u8>> {
        let token = uuid::Uuid::new_v4().as_bytes().to_vec();
        self.vault
            .lock()
            .unwrap()
            .insert(token.clone(), (master_key_id.to_string(), key.to_vec()));
        Ok(token)
    }

    fn unwrap_key(&self, wrapped_key: &[u8], master_key_id: &str) -> DeltaResult<Vec<u8>> {
        let vault = self.vault.lock().unwrap();
        let (wrapped_under, key) = vault.get(wrapped_key).ok_or_else(|| {
            DeltaTableError::Generic(format!(
                "in-memory KMS: unknown wrapped key for master key '{master_key_id}'"
            ))
        })?;
        if wrapped_under != master_key_id {
            return Err(DeltaTableError::Generic(format!(
                "in-memory KMS: key was wrapped under '{wrapped_under}', not '{master_key_id}'"
            )));
        }
        Ok(key.clone())
    }
}

/// The reference [`KmsEncryptionFactory`] over a fresh [`InMemoryKmsClient`].
pub fn mock_kms_factory() -> Arc<dyn EncryptionFactory> {
    Arc::new(KmsEncryptionFactory::new(Arc::new(
        InMemoryKmsClient::default(),
    )))
}
