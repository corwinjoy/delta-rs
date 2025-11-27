//! Utility functions for working across Delta tables

use chrono::DateTime;
use object_store::path::Path;
use object_store::ObjectMeta;
use std::path::Path as StdPath;
use url::Url;

use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::Add;

/// Return the uri of commit version.
///
/// ```rust
/// # use deltalake_core::logstore::*;
/// use object_store::path::Path;
/// let uri = commit_uri_from_version(1);
/// assert_eq!(uri, Path::from("_delta_log/00000000000000000001.json"));
/// ```
pub fn commit_uri_from_version(version: i64) -> Path {
    let version = format!("{version:020}.json");
    super::DELTA_LOG_PATH.child(version.as_str())
}

impl TryFrom<Add> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(value: Add) -> DeltaResult<Self> {
        (&value).try_into()
    }
}

impl TryFrom<&Add> for ObjectMeta {
    type Error = DeltaTableError;

    fn try_from(value: &Add) -> DeltaResult<Self> {
        let last_modified = DateTime::from_timestamp_millis(value.modification_time).ok_or(
            DeltaTableError::MetadataError(format!(
                "invalid modification_time: {:?}",
                value.modification_time
            )),
        )?;

        Ok(Self {
            // Preserve absolute filesystem paths and fully qualified URIs as-is.
            // Fall back to parsing relative paths via object_store::path::Path.
            location: {
                let p = value.path.as_str();
                // Try parsing as a URI first. If it parses, it's an absolute URI.
                // If it fails with RelativeUrlWithoutBase (or any other error),
                // treat it as a filesystem path and check if it's absolute.
                match Url::parse(p) {
                    Ok(_) => Path::from(p),
                    Err(_) => {
                        if StdPath::new(p).is_absolute() {
                            Path::from(p)
                        } else {
                            Path::parse(p)?
                        }
                    }
                }
            },
            last_modified,
            size: value.size as u64,
            e_tag: None,
            version: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_meta_from_add_action() {
        let add = Add {
            path: "x=A%252FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
                .to_string(),
            size: 123,
            modification_time: 123456789,
            data_change: true,
            stats: None,
            partition_values: Default::default(),
            tags: Default::default(),
            base_row_id: None,
            default_row_commit_version: None,
            deletion_vector: None,
            clustering_provider: None,
        };

        let meta: ObjectMeta = (&add).try_into().unwrap();
        assert_eq!(
            meta.location,
            Path::parse(
                "x=A%252FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
            )
            .unwrap()
        );
        assert_eq!(meta.size, 123);
        assert_eq!(meta.last_modified.timestamp_millis(), 123456789);
    }
}
