//! Writer-side encryption, driven by a table's `delta.encryption.*` properties.
//!
//! [`apply_encryption`] adds a [`WriterPropertiesLayer`](crate::datafile::WriterPropertiesLayer)
//! that asks the `EncryptionFactory` registered as `delta.encryption.kms_id` for each file's
//! keys. It runs after the caller's layers, so table encryption cannot be turned off.
//!
//! Needs the `encryption` cargo feature; without it, encrypted tables are refused.

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
/// The Parquet Key Management Tools crate behind the reference factory; implement its
/// `kms::KmsClient` (or `kms::AsyncKmsClient`) for your KMS.
#[cfg(feature = "encryption")]
pub use parquet_key_management;

/// `props` with the table's encryption, if `configuration` (its metadata configuration) has
/// any. An invalid configuration or an unregistered factory is an error, never a plaintext
/// write. The factory is looked up in `runtime_env`, then in the global registry.
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
