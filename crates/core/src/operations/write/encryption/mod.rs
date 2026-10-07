//! Writer-side encryption support driven by Delta table properties.
//!
//! Encryption configuration is read from the table's `delta.encryption.*` properties
//! (stored in the Delta log) rather than passed as a runtime API parameter.
//! This means that once a table is created with encryption properties all subsequent
//! write operations automatically encrypt output files — no per-operation configuration
//! is required from the caller.
//!
//! # Write-time key flow
//!
//! 1. [`writer_factory`] reads `delta.encryption.*` from the raw metadata configuration
//!    of the table's [`TableConfiguration`].
//! 2. It looks up the user-registered `EncryptionFactory` from DataFusion's
//!    `RuntimeEnv` using the `delta.encryption.kms_id` property value.
//! 3. It wraps the factory in a `KmsWriterPropertiesFactory`, which implements
//!    [`WriterPropertiesFactory`].
//! 4. Each new parquet file calls [`WriterPropertiesFactory::create_writer_properties`]
//!    **with the actual file path** so that the factory can derive the encryption key
//!    from the path (AAD — Additional Authenticated Data).
//!
//! Encrypting requires the `encryption` cargo feature: `enabled.rs` holds the KMS
//! factory and the factory registry, and `disabled.rs` stands in for builds without
//! the feature, where resolving an encrypted table's configuration fails instead of
//! writing plaintext. This module holds what both builds share.

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
/// Table encryption always takes precedence: for a table with `delta.encryption.*`
/// properties the factory encrypts every file, with `base_properties` (the caller's
/// `WriterProperties`, or the delta-rs SNAPPY defaults) supplying the non-crypto settings
/// such as compression and row-group sizing. For any other table the factory hands out
/// `base_properties` unchanged. Errors on an invalid encryption configuration or an
/// unregistered KMS factory rather than writing plaintext into an encrypted table.
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
