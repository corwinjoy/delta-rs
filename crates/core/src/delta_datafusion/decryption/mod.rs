//! Reader-side Parquet decryption for tables with `delta.encryption.*` properties.
//!
//! Every read path (scans, optimize, the change feed) derives its decryption options with
//! [`parquet_options_from_table_config`] and attaches the KMS factory to its Parquet sources
//! with [`Decryption`]. `enabled.rs` implements both for builds with the `encryption`
//! feature; `disabled.rs` makes them no-ops otherwise, since the protocol checker refuses
//! to read encrypted tables before any scan is planned.

#[cfg_attr(feature = "encryption", path = "enabled.rs")]
#[cfg_attr(not(feature = "encryption"), path = "disabled.rs")]
mod backend;

pub(crate) use backend::{Decryption, parquet_options_from_table_config};
