//! Engine-agnostic Delta read configuration.
//!
//! [`ReaderProperties`] is the single home for the parquet read options used by a
//! Delta scan. Today it centralizes construction of DataFusion's
//! [`TableParquetOptions`](datafusion::config::TableParquetOptions) for the (only)
//! DataFusion-backed read path, replacing the ad-hoc inline construction that was
//! duplicated across the scan and change-data-feed paths. Keeping it in one type
//! means any future read configuration is added — and applied — in exactly one
//! place.
//!
//! Planned extensions (intentionally left as documentation, not code, to keep this
//! type minimal until something uses them):
//!
//! * **Per-file decryption** — a static `FileDecryptionProperties`, or a KMS-style
//!   factory referenced by id. On the DataFusion path this maps onto
//!   `TableParquetOptions.crypto`; on the future DataFusion-free path it maps onto
//!   the parquet reader's decryption properties.
//! * **A `to_arrow_reader_options()` cast** — for the future DataFusion-free
//!   (kernel) reader, so the same properties drive parquet's `ArrowReaderBuilder`
//!   directly instead of going through DataFusion.
//!
//! This is the read-side counterpart to the write-side
//! [`WriterProperties`](parquet::file::properties::WriterProperties) (and the
//! per-file writer-properties factory): one value type that carries the parquet
//! IO configuration and is cast into whatever the engine performing the IO needs.

/// Engine-agnostic parquet read configuration for a Delta scan.
///
/// The single seam where read / parquet-IO properties are configured, then cast
/// into the concrete options the reading engine requires. See the module
/// documentation for the planned extensions (per-file decryption, a
/// DataFusion-free cast).
#[derive(Clone, Debug, Default)]
pub struct ReaderProperties {
    // Planned fields (see module docs) are intentionally omitted until they are
    // used, e.g.:
    //   decryption: Option<Decryption>,
}

#[cfg(feature = "datafusion")]
impl ReaderProperties {
    /// Cast into DataFusion's [`TableParquetOptions`](datafusion::config::TableParquetOptions)
    /// for a `ParquetSource`, inheriting the session's parquet execution settings.
    ///
    /// This is the one place a Delta read builds its `TableParquetOptions`, so
    /// future read configuration (e.g. decryption) is applied uniformly to every
    /// `ParquetSource` (the main scan and each change-data-feed source).
    pub fn to_table_parquet_options(
        &self,
        session: &dyn datafusion::catalog::Session,
    ) -> datafusion::config::TableParquetOptions {
        datafusion::config::TableParquetOptions {
            global: session.config().options().execution.parquet.clone(),
            ..Default::default()
        }
    }
}
