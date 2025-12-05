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

/// Returns true if the provided string is either:
/// - a fully-qualified URI (e.g., s3://bucket/path, file:///tmp/x, abfss://...), or
/// - an absolute filesystem path on the current platform (e.g., /usr/bin, C:\\data\\file).
///
/// This helper consolidates common absolute path checks scattered throughout the codebase.
pub fn is_absolute_uri_or_path(s: &str) -> bool {
    if Url::parse(s).is_ok() {
        true
    } else {
        StdPath::new(s).is_absolute()
    }
}

/// Normalize a data file path for the local file scheme.
///
/// This consolidates common logic used in multiple places to handle paths
/// when the table location uses the `file://` scheme:
/// - If `input` is a `file://` URI, convert it to a platform filesystem path.
/// - If `input` is a different fully-qualified URI or an absolute filesystem path,
///   keep it as-is.
/// - Otherwise, treat `input` as a relative path and join it under the filesystem
///   path of `root` (the table location), returning the absolute filesystem path
///   as a UTF-8 string (lossy if necessary).
pub fn normalize_path_for_file_scheme(root: &Url, input: &str) -> String {
    // Attempt to parse as URL first
    match Url::parse(input) {
        Ok(url) if url.scheme() == "file" => {
            // Convert file URI to filesystem path
            url.path().to_string()
        }
        Ok(_) => {
            // Some other URI scheme; keep as-is
            input.to_string()
        }
        Err(_) => {
            // Not a URI — could be absolute filesystem path or relative
            if is_absolute_uri_or_path(input) {
                // Absolute filesystem path: keep as-is
                input.to_string()
            } else {
                // Relative path: join under the table root directory
                let root_fs_path = root
                    .to_file_path()
                    .unwrap_or_else(|_| StdPath::new(root.path()).to_path_buf());
                // Ensure we treat input as relative by stripping any leading separators
                let rel = input.trim_start_matches(['/', '\\']);
                let full_path = root_fs_path.join(rel);
                full_path
                    .to_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| full_path.to_string_lossy().into_owned())
            }
        }
    }
}

/// For non-`file` schemes, strip the table root prefix from a fully-qualified URI
/// so that returned paths are relative to the table-scoped object store.
///
/// - If `input` begins with `root` followed by a `/`, returns the suffix after that `/`.
/// - If `input` is exactly equal to `root` (ignoring trailing `/` on `root`), returns an empty string.
/// - Otherwise, returns `input` unchanged.
pub fn strip_table_root_from_full_uri(root: &Url, input: &str) -> String {
    let root_str = root.as_str().trim_end_matches('/');
    let root_with_sep = format!("{}/", root_str);
    if input.starts_with(&root_with_sep) {
        input[root_with_sep.len()..].to_string()
    } else if input == root_str {
        String::new()
    } else {
        input.to_string()
    }
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
            // Preserve paths carefully to avoid double percent-encoding:
            // - For absolute filesystem paths (e.g., "/tmp/x"), use Path::parse to
            //   preserve any existing percent-escapes in partition directories.
            // - For fully-qualified URIs (e.g., file:///tmp/x), we expect earlier
            //   normalization to have converted them into filesystem paths for the
            //   file scheme; other schemes should normally use relative paths.
            // - For relative paths (typical case), use Path::parse.
            location: {
                let p = value.path.as_str();
                Path::parse(p)?
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
