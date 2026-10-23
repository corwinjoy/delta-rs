//! Reader-side Parquet decryption for tables with `delta.encryption.*` properties.
//!
//! Every read path (scans, optimize, the change feed) resolves a [`Decryption`] from the
//! table's configuration and applies it to its Parquet sources. `enabled.rs` implements it
//! for builds with the `encryption` feature; `disabled.rs` makes it a no-op otherwise, since
//! the protocol checker refuses to read encrypted tables before any scan is planned.

#[cfg_attr(feature = "encryption", path = "enabled.rs")]
#[cfg_attr(not(feature = "encryption"), path = "disabled.rs")]
mod backend;

pub(crate) use backend::Decryption;
