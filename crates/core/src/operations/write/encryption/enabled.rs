//! The encrypting [`WriterPropertiesLayer`] and the process-wide [`EncryptionFactory`]
//! registry.

use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use datafusion::execution::runtime_env::RuntimeEnv;
use parquet::file::properties::WriterPropertiesBuilder;

use crate::datafile::{FileContext, WriterPropertiesLayer};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::table::config::EncryptionConfig;

/// Asks the table's [`EncryptionFactory`] for each file's keys, passing the table's
/// properties with `column_keys` resolved to the file's parquet leaf paths (see
/// [`EncryptionConfig::writer_factory_options`]). Adds only the encryption properties.
#[derive(Debug)]
pub(super) struct KmsEncryptionLayer {
    pub(super) encryption_factory: Arc<dyn EncryptionFactory>,
    pub(super) enc: EncryptionConfig,
}

#[async_trait]
impl WriterPropertiesLayer for KmsEncryptionLayer {
    async fn apply(
        &self,
        builder: WriterPropertiesBuilder,
        file: FileContext<'_>,
    ) -> DeltaResult<WriterPropertiesBuilder> {
        // Leaf names depend on `coerce_types`, which only the built properties expose.
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

/// [`EncryptionFactory`]s by `kms_id`, for operations whose own DataFusion sessions cannot
/// see the user's.
static GLOBAL_FACTORY_REGISTRY: LazyLock<DashMap<String, Arc<dyn EncryptionFactory>>> =
    LazyLock::new(DashMap::new);

/// Register `factory` under `id`, the `delta.encryption.kms_id` of the tables it serves, for
/// the rest of the process. Last write wins (with a warning), and the factory receives every
/// key request for those tables, so treat registration as privileged setup.
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

/// The factory registered as `kms_id` in `runtime_env`, else in the global registry; an
/// error if neither.
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
