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

/// Returns true if the provided string is either a fully-qualified URI or
/// an absolute filesystem path for the current platform.
pub fn is_absolute_uri_or_path(s: &str) -> bool {
    if Url::parse(s).is_ok() {
        true
    } else {
        StdPath::new(s).is_absolute()
    }
}

/// Normalize a data file path for tables using the file scheme.
///
/// Rules:
/// - If input is a file:// URI, convert to a platform filesystem path.
/// - If input is another fully-qualified URI or an absolute filesystem path, keep as-is.
/// - Otherwise, treat input as relative and join under the filesystem path of `root`.
pub fn normalize_path_for_file_scheme(root: &Url, input: &str) -> String {
    match Url::parse(input) {
        Ok(url) if url.scheme() == "file" => url.path().to_string(),
        Ok(_) => input.to_string(),
        Err(_) => {
            if StdPath::new(input).is_absolute() {
                input.to_string()
            } else {
                let root_fs_path = root
                    .to_file_path()
                    .unwrap_or_else(|_| StdPath::new(root.path()).to_path_buf());
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

/// For non-file schemes, if `input` is a fully-qualified URI beginning with `root`,
/// strip the table root prefix so the result is relative to the table root.
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

/// For object store paths (e.g., S3 and S3-compatible HTTP endpoints), if `input`
/// is a fully-qualified URI that points to the same bucket as `root`, return the
/// object key relative to the bucket root. Otherwise, return `input` unchanged.
///
/// This function is scheme-tolerant: it can match `s3://bucket/key` against
/// `http(s)://endpoint/bucket/` (path-style) or `http(s)://bucket.endpoint/`
/// (virtual-hosted-style) as long as the bucket names are the same.
pub fn relativize_uri_to_bucket_root(root: &Url, input: &str) -> String {
    // If it's not a URI, return as-is
    let Ok(input_url) = Url::parse(input) else {
        return input.to_string();
    };

    // Try to extract bucket names from both URLs. If either is not a recognizable
    // object-store style URL, leave input unchanged.
    let Some(root_bucket) = extract_bucket_name(root) else {
        return input.to_string();
    };
    let Some(input_bucket) = extract_bucket_name(&input_url) else {
        return input.to_string();
    };

    if root_bucket != input_bucket {
        return input.to_string();
    }

    // Same bucket: return the key (path relative to the bucket root) without leading '/'
    bucket_key_from_url(&input_url).unwrap_or_else(|| input.to_string())
}

/// Attempt to extract an object key relative to bucket root from a URL.
/// Returns `None` if the URL doesn't look like an object-store style URL.
fn bucket_key_from_url(url: &Url) -> Option<String> {
    let path = url.path().trim_start_matches('/');
    if url.scheme() == "s3" {
        // s3://bucket/key...
        // First path segment is the key directly; bucket is in host
        return Some(path.to_string());
    }

    // HTTP(S) S3-compatible: support both virtual-hosted-style and path-style
    if url.scheme() == "http" || url.scheme() == "https" {
        let host = url.host_str().unwrap_or("");
        // virtual-hosted-style: bucket.s3.amazonaws.com or bucket.localstack
        // In this style, entire path is the key
        if looks_like_virtual_hosted_style(host) {
            return Some(path.to_string());
        }
        // path-style: http://endpoint/bucket/key...
        if let Some((_, rest)) = path.split_once('/') {
            return Some(rest.to_string());
        } else {
            // Only bucket with no key
            return Some(String::new());
        }
    }

    None
}

/// Extract bucket name from s3 or s3-compatible http(s) URL.
fn extract_bucket_name(url: &Url) -> Option<String> {
    if url.scheme() == "s3" {
        return url.host_str().map(|s| s.to_string());
    }
    if url.scheme() == "http" || url.scheme() == "https" {
        let host = url.host_str().unwrap_or("");
        if let Some(bucket) = bucket_from_virtual_hosted_style(host) {
            return Some(bucket.to_string());
        }
        // path-style: first segment of path is bucket
        let path = url.path().trim_start_matches('/');
        if let Some((bucket, _)) = path.split_once('/') {
            if !bucket.is_empty() {
                return Some(bucket.to_string());
            }
        } else if !path.is_empty() {
            // Only bucket without key
            return Some(path.to_string());
        }
    }
    None
}

fn looks_like_virtual_hosted_style(host: &str) -> bool {
    // If host starts with "bucket." and contains known s3 or custom domains, treat as virtual-hosted-style.
    // This is a heuristic: anything with a dot is acceptable, since we only need key extraction
    // (host does not participate in the resulting key, and bucket is validated separately).
    host.contains('.')
}

fn bucket_from_virtual_hosted_style(host: &str) -> Option<&str> {
    // bucket is up to first '.'
    host.split_once('.').map(|(b, _)| b).filter(|b| !b.is_empty())
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
            // IMPORTANT: `ObjectMeta` is only constructed at the boundary where
            // a concrete object store is used. At that point, `value.path` must
            // already be normalized (either relative to table root for remote
            // schemes or an absolute filesystem path for file scheme).
            // The path from the Delta log is already percent-encoded per the Delta
            // protocol. Using `Path::from` instead of `Path::parse` avoids
            // double-encoding (e.g., `%20` becoming `%2520`).
            location: Path::from(value.path.as_str()),
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
            Path::from(
                "x=A%252FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
            )
        );
        assert_eq!(meta.size, 123);
        assert_eq!(meta.last_modified.timestamp_millis(), 123456789);
    }
}
