//! Mock KMS implementation for testing encryption via delta table properties.
//!
//! This module is **not part of the stable public API**. It lives in `test_utils` and is
//! compiled in non-test builds only to allow downstream integration-test crates to depend on it
//! without pulling in a separate crate. Do not rely on it for production use.
//!
//! # Usage
//!
//! 1. Create a [`MockKmsFactory`] and register it with a DataFusion session using
//!    [`datafusion::execution::RuntimeEnv::register_parquet_encryption_factory`].
//! 2. Create a Delta table with `delta.encryption.kms.id` set to the same ID.
//! 3. All subsequent read/write operations on the table will use the registered factory.
//!
//! ```rust,ignore
//! // Register factory at startup
//! let factory = Arc::new(MockKmsFactory::new());
//! session.runtime_env().register_parquet_encryption_factory("test-kms", factory.clone());
//!
//! // Create encrypted table
//! table.create()
//!     .with_property("delta.encryption.kms.id", "test-kms")
//!     .with_property("delta.encryption.footer.key", "my-key")
//!     .await?;
//! ```

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::config::EncryptionFactoryOptions;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use object_store::path::Path;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;

/// Mock encryption factory for use in tests.
///
/// Generates a unique encryption key for each file and stores it for later decryption.
/// Keys are keyed by filename (basename) so path-prefix differences between write and read
/// are handled correctly.
#[derive(Debug, Default)]
pub struct MockKmsFactory {
    encryption_keys: Mutex<HashMap<Path, Vec<u8>>>,
    counter: AtomicU64,
}

impl MockKmsFactory {
    pub fn new() -> Self {
        Self {
            encryption_keys: Mutex::new(HashMap::new()),
            counter: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl EncryptionFactory for MockKmsFactory {
    async fn get_file_encryption_properties(
        &self,
        _config: &EncryptionFactoryOptions,
        _schema: &Arc<ArrowSchema>,
        file_path: &Path,
    ) -> datafusion::error::Result<Option<Arc<FileEncryptionProperties>>> {
        let file_idx = self.counter.fetch_add(1, Ordering::Relaxed);
        let mut key = [0u8; 16];
        key[..8].copy_from_slice(&file_idx.to_le_bytes());
        let key = key.to_vec();

        let mut keys = self.encryption_keys.lock().unwrap();
        // Use just the filename to handle path prefix differences between write and read.
        let filename = Path::from(file_path.filename().unwrap_or(file_path.as_ref()));
        keys.insert(filename, key.clone());

        let props = FileEncryptionProperties::builder(key).build()?;
        Ok(Some(props))
    }

    async fn get_file_decryption_properties(
        &self,
        _config: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> datafusion::error::Result<Option<Arc<FileDecryptionProperties>>> {
        let keys = self.encryption_keys.lock().unwrap();
        let filename = Path::from(file_path.filename().unwrap_or(file_path.as_ref()));
        let key = keys.get(&filename).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "No encryption key found for file {file_path:?}"
            ))
        })?;
        let props = FileDecryptionProperties::builder(key.clone()).build()?;
        Ok(Some(props))
    }
}
