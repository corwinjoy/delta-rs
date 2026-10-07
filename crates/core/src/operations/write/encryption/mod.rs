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
//! 1. [`WriterEncryptionConfig::from_config`] reads `delta.encryption.*` from the raw
//!    metadata configuration of the table's [`TableConfiguration`].
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
pub use backend::{
    get_encryption_factory, register_encryption_factory, resolve_encryption_factory,
};
#[cfg(feature = "encryption")]
pub use kms::{KmsClient, KmsEncryptionFactory};
// Re-export the factory types that are defined in the non-datafusion `writer_factory` module
// so callers can keep importing them from this module.
pub use crate::writer::writer_factory::{
    WriterPropertiesFactory, WriterPropertiesFactoryRef, default_writer_properties_factory,
    factory_from_writer_properties, snappy_writer_properties,
};

// ---------------------------------------------------------------------------
// WriterEncryptionConfig — resolved from TableConfiguration + Session
// ---------------------------------------------------------------------------

/// Encryption configuration for the write path, resolved from Delta table properties.
///
/// Create via [`WriterEncryptionConfig::from_config`]; then pass
/// [`WriterEncryptionConfig::factory`] to [`WriterConfig::new`].
#[derive(Debug, Default)]
pub struct WriterEncryptionConfig {
    /// `None` when the table has no encryption properties.
    pub factory: Option<WriterPropertiesFactoryRef>,
}

impl WriterEncryptionConfig {
    /// Resolve from a [`TableConfiguration`] (used in `write_exec_plan` which receives
    /// `table_config: &TableConfiguration` directly).
    ///
    /// `base_properties` supplies the non-crypto writer settings (compression,
    /// row-group sizing, statistics, …) the encrypted factory encodes files
    /// with — pass the caller's `WriterProperties` so an encrypted table honors
    /// them; `None` uses the delta-rs SNAPPY defaults. Encryption itself always
    /// comes from the table properties and cannot be overridden by the caller.
    pub fn from_config(
        config: &TableConfiguration,
        session: &dyn Session,
        base_properties: Option<WriterProperties>,
    ) -> DeltaResult<Self> {
        let env = session.runtime_env();
        Self::from_configuration(
            config.metadata().configuration(),
            Some(env.as_ref()),
            base_properties,
        )
    }

    /// Resolve from a table's raw metadata `configuration` with an optional `RuntimeEnv`.
    ///
    /// The env (a session's or a `TaskContext`'s) is checked first; without one
    /// (e.g. the legacy writers, which have no DataFusion context), the factory
    /// is looked up in the global registry only.
    pub fn from_configuration(
        configuration: &HashMap<String, String>,
        runtime_env: Option<&RuntimeEnv>,
        base_properties: Option<WriterProperties>,
    ) -> DeltaResult<Self> {
        // try_from_configuration errors when the encryption configuration is
        // invalid, preventing silent plaintext writes on misconfigured tables.
        let Some(enc) = EncryptionConfig::try_from_configuration(configuration)? else {
            return Ok(Self { factory: None });
        };
        Ok(Self {
            factory: Some(backend::resolve(enc, runtime_env, base_properties)?),
        })
    }
}
