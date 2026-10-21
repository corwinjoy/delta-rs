//! Writer encryption for builds without the `encryption` feature: there is no KMS
//! support, so encrypted tables are refused rather than written as plaintext.

use datafusion::execution::runtime_env::RuntimeEnv;

use crate::datafile::DeltaWriterProperties;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::table::config::EncryptionConfig;

/// Always fails: this build cannot encrypt, and must not write plaintext files into an
/// encrypted table.
pub(super) fn apply(
    _props: DeltaWriterProperties,
    _enc: EncryptionConfig,
    _runtime_env: Option<&RuntimeEnv>,
) -> DeltaResult<DeltaWriterProperties> {
    Err(DeltaTableError::Generic(
        "This table is encrypted (delta.encryption.* properties are set), but delta-rs was \
         built without the `encryption` feature"
            .to_string(),
    ))
}
