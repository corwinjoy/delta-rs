//! The KMS-backed [`WriterPropertiesLayer`] and the process-wide [`EncryptionFactory`]
//! registry.

use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use datafusion::execution::runtime_env::RuntimeEnv;
use parquet::file::properties::WriterPropertiesBuilder;

use crate::datafile::{DeltaWriterProperties, FileContext, WriterPropertiesLayer};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::table::config::EncryptionConfig;

/// `props` with a layer that encrypts every file through the [`EncryptionFactory`] named
/// by the table's `kms_id` (see [`resolve_encryption_factory`]).
pub(super) fn apply(
    props: DeltaWriterProperties,
    enc: EncryptionConfig,
    runtime_env: Option<&RuntimeEnv>,
) -> DeltaResult<DeltaWriterProperties> {
    let encryption_factory = resolve_encryption_factory(&enc.kms_id, runtime_env)?;
    Ok(props.with_layer(KmsEncryptionLayer {
        encryption_factory,
        enc,
    }))
}

/// A [`WriterPropertiesLayer`] that gets each file's encryption keys from the
/// [`EncryptionFactory`] registered for the table's KMS. The factory gets the table's
/// properties as options (see [`EncryptionConfig::writer_factory_options`]), with
/// `column_keys` resolved to the file's parquet leaf paths, so a factory keys the paths
/// as listed. The layer only adds the file encryption properties: the codec and row-group
/// bounds the file was planned with stay.
#[derive(Debug)]
struct KmsEncryptionLayer {
    encryption_factory: Arc<dyn EncryptionFactory>,
    enc: EncryptionConfig,
}

#[async_trait]
impl WriterPropertiesLayer for KmsEncryptionLayer {
    async fn apply(
        &self,
        builder: WriterPropertiesBuilder,
        file: FileContext<'_>,
    ) -> DeltaResult<WriterPropertiesBuilder> {
        // Leaf names depend on the writer's `coerce_types`, which only the built properties
        // expose.
        let props = builder.build();
        let file_enc = EncryptionConfig {
            column_keys: self
                .enc
                .column_key_leaves(file.schema, props.coerce_types())?,
            ..self.enc.clone()
        };
        let encryption_props = self
            .encryption_factory
            .get_file_encryption_properties(
                &file_enc.writer_factory_options(),
                file.schema,
                file.path,
            )
            .await?
            .ok_or_else(|| {
                DeltaTableError::Generic(format!(
                    "The EncryptionFactory returned no file encryption properties for '{}', \
                     but the table's delta.encryption.* properties require encryption; \
                     refusing to write a plaintext file",
                    file.path
                ))
            })?;
        Ok(props
            .into_builder()
            .with_file_encryption_properties(encryption_props))
    }
}

/// Process-wide registry of [`EncryptionFactory`]s by `kms_id`, for the operations that
/// create their own DataFusion sessions and so cannot see a user's `SessionContext`.
static GLOBAL_FACTORY_REGISTRY: LazyLock<DashMap<String, Arc<dyn EncryptionFactory>>> =
    LazyLock::new(DashMap::new);

/// Register an [`EncryptionFactory`] under `id`, the `delta.encryption.kms_id` of the tables
/// that should use it, for the rest of the process.
///
/// The registry is last-write-wins: any code in the process can re-register an id and then
/// receives every key request for its tables (key IDs and KMS configuration, never key
/// material). Re-registering logs a warning. Treat registration as privileged setup code.
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

/// The [`EncryptionFactory`] registered as `kms_id`: `runtime_env`'s, if any, else the
/// global registry's. Errors when neither has it.
pub fn resolve_encryption_factory(
    kms_id: &str,
    runtime_env: Option<&RuntimeEnv>,
) -> DeltaResult<Arc<dyn EncryptionFactory>> {
    runtime_env
        .and_then(|env| env.parquet_encryption_factory(kms_id).ok())
        .or_else(|| {
            GLOBAL_FACTORY_REGISTRY
                .get(kms_id)
                .map(|e| Arc::clone(e.value()))
        })
        .ok_or_else(|| {
            DeltaTableError::Generic(format!(
                "No EncryptionFactory registered for kms_id '{kms_id}'. Register one via \
                 `deltalake_core::operations::write::encryption::register_encryption_factory`."
            ))
        })
}
