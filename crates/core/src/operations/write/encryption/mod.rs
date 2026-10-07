//! Writer-side encryption, driven by a table's `delta.encryption.*` properties.
//!
//! [`writer_factory`] reads the properties from the raw metadata configuration, finds the
//! `EncryptionFactory` registered as `delta.encryption.kms_id` (in the session's
//! `RuntimeEnv`, else the process-wide registry) and wraps it in a
//! [`WriterPropertiesFactory`] that asks it for each file's encryption keys, passing the
//! file's table-relative path. Nothing is configured per operation: once a table is created
//! encrypted, every write encrypts.
//!
//! Encrypting needs the `encryption` cargo feature: `enabled.rs` holds the KMS-backed
//! factory and the registry, `disabled.rs` refuses encrypted tables instead of writing
//! plaintext. This module holds what both builds share.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::Session;
use datafusion::execution::runtime_env::RuntimeEnv;
use delta_kernel::table_configuration::TableConfiguration;
use parquet::file::properties::WriterProperties;

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
// Re-export the factory types that are defined in the non-datafusion `writer_factory` module
// so callers can keep importing them from this module.
pub use crate::writer::writer_factory::{
    WriterPropertiesFactory, WriterPropertiesFactoryRef, default_writer_properties_factory,
    factory_from_writer_properties,
};

/// The [`WriterPropertiesFactory`] a table's files are written with.
///
/// For a table with `delta.encryption.*` properties it encrypts every file, with
/// `base_properties` (the caller's `WriterProperties`, or the delta-rs SNAPPY defaults) as
/// the non-crypto settings: table encryption always wins over the caller's settings. For
/// any other table it hands out `base_properties` unchanged. An invalid configuration or
/// an unregistered KMS factory is an error, never a plaintext write.
pub fn writer_factory(
    config: &TableConfiguration,
    session: &dyn Session,
    base_properties: Option<WriterProperties>,
) -> DeltaResult<WriterPropertiesFactoryRef> {
    let env = session.runtime_env();
    writer_factory_from_configuration(
        config.metadata().configuration(),
        Some(env.as_ref()),
        base_properties,
    )
}

/// [`writer_factory`] for a table's raw metadata `configuration` and an optional
/// `RuntimeEnv`. The env (a session's or a `TaskContext`'s) is checked for the KMS factory
/// first; without one (e.g. the legacy writers, which have no DataFusion context), only the
/// global registry is.
pub fn writer_factory_from_configuration(
    configuration: &HashMap<String, String>,
    runtime_env: Option<&RuntimeEnv>,
    base_properties: Option<WriterProperties>,
) -> DeltaResult<WriterPropertiesFactoryRef> {
    match EncryptionConfig::try_from_configuration(configuration)? {
        Some(enc) => backend::resolve(enc, runtime_env, base_properties),
        None => Ok(Arc::new(base_properties.unwrap_or_else(
            crate::writer::writer_factory::snappy_writer_properties,
        ))),
    }
}
