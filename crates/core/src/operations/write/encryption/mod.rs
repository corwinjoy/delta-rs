//! Writer-side encryption, driven by a table's `delta.encryption.*` properties.
//!
//! [`apply_encryption`] finds the `EncryptionFactory` registered as `delta.encryption.kms_id`
//! (in the session's `RuntimeEnv`, else the process-wide registry) and adds a
//! [`WriterPropertiesLayer`](crate::datafile::WriterPropertiesLayer) that asks it for each
//! file's keys. The layer runs after the caller's, so once a table is created encrypted
//! every write encrypts, whatever parquet properties the caller set.
//!
//! Encrypting needs the `encryption` cargo feature: `enabled.rs` holds the layer and the
//! registry, `kms.rs` the reference factory over [`parquet_key_management`]. Without the
//! feature, encrypted tables are refused rather than written as plaintext.

use std::collections::HashMap;

use datafusion::execution::runtime_env::RuntimeEnv;

use crate::datafile::DeltaWriterProperties;
use crate::errors::DeltaResult;
use crate::table::config::EncryptionConfig;

#[cfg(feature = "encryption")]
mod enabled;
#[cfg(feature = "encryption")]
pub mod kms;

#[cfg(feature = "encryption")]
pub use enabled::{register_encryption_factory, resolve_encryption_factory};
#[cfg(feature = "encryption")]
pub use kms::KmsEncryptionFactory;
/// The Parquet Key Management Tools crate the reference factory is built on: implement its
/// `kms::KmsClient` (or `kms::AsyncKmsClient`) for your KMS.
#[cfg(feature = "encryption")]
pub use parquet_key_management;

/// `props` with the table's encryption, if `configuration` (the table's metadata
/// configuration) has any; `props` unchanged otherwise. An invalid configuration or an
/// unregistered KMS factory is an error, never a plaintext write.
///
/// The factory is looked up in `runtime_env` first, then in the global registry, which is
/// all the legacy writers have. The DataFusion write paths call this through
/// [`DeltaWriterProperties::for_table`].
pub fn apply_encryption(
    props: DeltaWriterProperties,
    configuration: &HashMap<String, String>,
    runtime_env: Option<&RuntimeEnv>,
) -> DeltaResult<DeltaWriterProperties> {
    let Some(enc) = EncryptionConfig::try_from_configuration(configuration)? else {
        return Ok(props);
    };
    #[cfg(feature = "encryption")]
    {
        enabled::apply(props, enc, runtime_env)
    }
    #[cfg(not(feature = "encryption"))]
    {
        let _ = (props, enc, runtime_env);
        Err(crate::errors::DeltaTableError::Generic(
            "This table is encrypted (delta.encryption.* properties are set), but delta-rs \
             was built without the `encryption` feature"
                .to_string(),
        ))
    }
}
