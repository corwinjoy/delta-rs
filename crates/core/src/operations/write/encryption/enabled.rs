//! Writer encryption for builds with the `encryption` feature: the KMS-backed
//! [`WriterPropertiesFactory`] and the process-wide [`EncryptionFactory`] registry.

use std::sync::{Arc, LazyLock};

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::config::EncryptionFactoryOptions;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use datafusion::execution::runtime_env::RuntimeEnv;
use object_store::path::Path;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};

use super::{WriterPropertiesFactory, WriterPropertiesFactoryRef};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::table::config::EncryptionConfig;
use crate::writer::writer_factory::snappy_writer_properties;

/// Build the writer factory for an encrypted table, with the [`EncryptionFactory`] named by
/// its `kms_id` (see [`resolve_encryption_factory`]).
pub(super) fn resolve(
    enc: EncryptionConfig,
    runtime_env: Option<&RuntimeEnv>,
    base_properties: Option<WriterProperties>,
) -> DeltaResult<WriterPropertiesFactoryRef> {
    let encryption_factory = resolve_encryption_factory(&enc.kms_id, runtime_env)?;
    Ok(Arc::new(KmsWriterPropertiesFactory {
        base_properties: base_properties.unwrap_or_else(snappy_writer_properties),
        encryption_factory,
        factory_options: enc.writer_factory_options(),
    }))
}

// ---------------------------------------------------------------------------
// KmsWriterPropertiesFactory — fetches per-file keys from a KMS via DataFusion
// ---------------------------------------------------------------------------

/// A [`WriterPropertiesFactory`] that derives per-file encryption keys from a KMS by
/// delegating to the DataFusion [`EncryptionFactory`] registered in `RuntimeEnv`.
///
/// Key material (footer key, column keys, plaintext-footer flag) is encoded in
/// `factory_options` and forwarded to the factory — see
/// [`EncryptionConfig::writer_factory_options`].
/// The factory itself is responsible for deriving the actual per-file key material.
#[derive(Debug)]
struct KmsWriterPropertiesFactory {
    base_properties: WriterProperties,
    encryption_factory: Arc<dyn EncryptionFactory>,
    factory_options: EncryptionFactoryOptions,
}

#[async_trait]
impl WriterPropertiesFactory for KmsWriterPropertiesFactory {
    fn base_properties(&self) -> &WriterProperties {
        &self.base_properties
    }

    fn with_base_properties(
        &self,
        properties: WriterProperties,
    ) -> Option<WriterPropertiesFactoryRef> {
        Some(Arc::new(Self {
            base_properties: properties,
            encryption_factory: Arc::clone(&self.encryption_factory),
            factory_options: self.factory_options.clone(),
        }))
    }

    async fn create_writer_properties(
        &self,
        file_path: &Path,
        file_schema: &Arc<ArrowSchema>,
    ) -> DeltaResult<WriterProperties> {
        let encryption_props = self
            .encryption_factory
            .get_file_encryption_properties(&self.factory_options, file_schema, file_path)
            .await?;

        // This factory only exists because the table's properties declare
        // encryption, so a factory that produces no encryption properties is a
        // misconfiguration — erroring here prevents silently writing plaintext
        // files into an encrypted table.
        let Some(enc_props) = encryption_props else {
            return Err(DeltaTableError::Generic(format!(
                "The EncryptionFactory returned no file encryption properties for '{file_path}', \
                 but the table's delta.encryption.* properties require encryption; \
                 refusing to write a plaintext file"
            )));
        };

        let builder: WriterPropertiesBuilder = self.base_properties.clone().into();
        Ok(builder.with_file_encryption_properties(enc_props).build())
    }
}

// ---------------------------------------------------------------------------
// Global EncryptionFactory registry
// ---------------------------------------------------------------------------

/// Process-wide registry for [`EncryptionFactory`] implementations.
///
/// Delta-rs operations create their own internal DataFusion sessions, which do not
/// automatically inherit factories registered in a user-created `SessionContext`.
/// This global registry bridges that gap: register your factory once here and all
/// delta-rs operations (write, read, optimize, etc.) will find it automatically.
///
/// ```rust,ignore
/// use deltalake_core::operations::write::encryption::register_encryption_factory;
///
/// register_encryption_factory("my-kms", Arc::new(MyFactory::new()));
/// ```
static GLOBAL_FACTORY_REGISTRY: LazyLock<DashMap<String, Arc<dyn EncryptionFactory>>> =
    LazyLock::new(DashMap::new);

/// Register an [`EncryptionFactory`] in the process-wide registry.
///
/// The `id` must match the value of `delta.encryption.kms_id` on any table that should
/// use this factory.  Registration persists for the lifetime of the process; there is
/// no unregistration, so a factory (and any credentials it holds) lives until exit.
///
/// # Trust model
///
/// The registry is process-global and last-write-wins: any code in the process can
/// re-register an id and will then receive every key-derivation request for tables
/// using that id (key identifiers, column-key maps, KMS configuration — not key
/// material, which stays inside the factory). Registering over an existing id logs
/// a warning. Treat factory registration as privileged setup code.
pub fn register_encryption_factory(id: impl Into<String>, factory: Arc<dyn EncryptionFactory>) {
    let id = id.into();
    if GLOBAL_FACTORY_REGISTRY
        .insert(id.clone(), factory)
        .is_some()
    {
        tracing::warn!(
            "replaced the previously registered EncryptionFactory for kms_id '{id}'; \
             all future key-derivation requests for that id go to the new factory"
        );
    }
}

/// Look up a previously registered [`EncryptionFactory`] by id.
fn get_encryption_factory(id: &str) -> Option<Arc<dyn EncryptionFactory>> {
    GLOBAL_FACTORY_REGISTRY
        .get(id)
        .map(|e| Arc::clone(e.value()))
}

/// The [`EncryptionFactory`] registered as `kms_id`: the one in `runtime_env`, if any, else
/// the one in the global registry, which operations that create their own internal sessions
/// rely on. Errors when neither has it.
pub fn resolve_encryption_factory(
    kms_id: &str,
    runtime_env: Option<&RuntimeEnv>,
) -> DeltaResult<Arc<dyn EncryptionFactory>> {
    runtime_env
        .and_then(|env| env.parquet_encryption_factory(kms_id).ok())
        .or_else(|| get_encryption_factory(kms_id))
        .ok_or_else(|| {
            DeltaTableError::Generic(format!(
                "No EncryptionFactory registered for kms_id '{kms_id}'. Register one via \
                 `deltalake_core::operations::write::encryption::register_encryption_factory`."
            ))
        })
}
