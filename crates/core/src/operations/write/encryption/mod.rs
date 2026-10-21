//! Writer-side encryption, driven by a table's `delta.encryption.*` properties.
//!
//! [`apply_encryption`] reads the properties from the raw metadata configuration, finds the
//! `EncryptionFactory` registered as `delta.encryption.kms_id` (in the session's
//! `RuntimeEnv`, else the process-wide registry) and adds a [`WriterPropertiesLayer`] to the
//! operation's [`DeltaWriterProperties`] that asks it for each file's encryption keys,
//! passing the file's table-relative path. Nothing is configured per operation: once a
//! table is created encrypted, every write encrypts, whatever parquet properties the caller
//! set, since the layer runs after them.
//!
//! Encrypting needs the `encryption` cargo feature: `enabled.rs` holds the KMS-backed
//! layer and the registry, `disabled.rs` refuses encrypted tables instead of writing
//! plaintext. This module holds what both builds share.

use std::collections::HashMap;

use datafusion::catalog::Session;
use datafusion::execution::runtime_env::RuntimeEnv;
use delta_kernel::table_configuration::TableConfiguration;

use crate::datafile::DeltaWriterProperties;
#[cfg(doc)]
use crate::datafile::WriterPropertiesLayer;
use crate::errors::DeltaResult;
use crate::table::config::EncryptionConfig;

#[cfg_attr(feature = "encryption", path = "enabled.rs")]
#[cfg_attr(not(feature = "encryption"), path = "disabled.rs")]
mod backend;

#[cfg(feature = "encryption")]
pub mod kms;

#[cfg(feature = "encryption")]
pub use backend::{register_encryption_factory, resolve_encryption_factory};
#[cfg(feature = "encryption")]
pub use kms::{KmsClient, KmsEncryptionFactory};

/// `props` with the table's encryption, if it has any.
///
/// For a table with `delta.encryption.*` properties, a layer that encrypts every file is
/// added after the layers `props` already has, so the caller's parquet properties still
/// apply but cannot turn encryption off: table encryption always wins. For any other
/// table `props` is returned unchanged. An invalid configuration or an unregistered KMS
/// factory is an error, never a plaintext write.
pub fn apply_encryption(
    props: DeltaWriterProperties,
    config: &TableConfiguration,
    session: &dyn Session,
) -> DeltaResult<DeltaWriterProperties> {
    let env = session.runtime_env();
    apply_encryption_from_configuration(props, config.metadata().configuration(), Some(env))
}

/// [`apply_encryption`] for a table's raw metadata `configuration` and an optional
/// `RuntimeEnv`. The env (a session's or a `TaskContext`'s) is checked for the KMS factory
/// first; without one (e.g. the legacy writers, which have no DataFusion context), only the
/// global registry is.
pub fn apply_encryption_from_configuration(
    props: DeltaWriterProperties,
    configuration: &HashMap<String, String>,
    runtime_env: Option<&RuntimeEnv>,
) -> DeltaResult<DeltaWriterProperties> {
    match EncryptionConfig::try_from_configuration(configuration)? {
        Some(enc) => backend::apply(props, enc, runtime_env),
        None => Ok(props),
    }
}
