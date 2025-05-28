use aes_gcm::{AeadInPlace, Aes256Gcm, Key, Nonce, Tag};
use async_stream::try_stream;
use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_URL_SAFE};
use bytes::Bytes;
use ciborium::{from_reader, into_writer};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use moka::{future::Cache, ops::compute::Op};
use deltalake_core::storage::object_store;
use object_store::{path::Path, *};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteArray;
use sha3::Digest;
use std::{fmt::Debug, ops::Range, sync::Arc, time::Duration};

// use crate::{map_arc_error, sha3_256};
// copy from anda
fn map_arc_error(err: Arc<Error>) -> Error {
    match err.as_ref() {
        Error::NotFound { path, source } => Error::NotFound {
            path: path.clone(),
            source: source.to_string().into(),
        },
        Error::AlreadyExists { path, source } => Error::AlreadyExists {
            path: path.clone(),
            source: source.to_string().into(),
        },
        Error::Precondition { path, source } => Error::Precondition {
            path: path.clone(),
            source: source.to_string().into(),
        },
        Error::NotModified { path, source } => Error::NotModified {
            path: path.clone(),
            source: source.to_string().into(),
        },
        Error::PermissionDenied { path, source } => Error::PermissionDenied {
            path: path.clone(),
            source: source.to_string().into(),
        },
        Error::Unauthenticated { path, source } => Error::Unauthenticated {
            path: path.clone(),
            source: source.to_string().into(),
        },
        err => Error::Generic {
            store: "MetaStore",
            source: err.to_string().into(),
        },
    }
}

fn sha3_256(data: &[u8]) -> [u8; 32] {
    let mut hasher = sha3::Sha3_256::new();
    hasher.update(data);
    hasher.finalize().into()
}


/// An object store implementation that provides transparent AES-256-GCM encryption and decryption
/// for stored objects.
///
/// `EncryptedStore` wraps another object store implementation and handles encryption/decryption
/// of data before it is passed to the underlying store. It also manages metadata for each object
/// to store encryption details.
///
/// # Features
/// - Transparent encryption/decryption using AES-256-GCM
/// - Chunked encryption for large objects
/// - Metadata caching for improved performance
/// - Optional conditional put operations
///
/// # Security considerations
///
/// This implementation uses AES-256-GCM for encryption which provides:
/// - Confidentiality: Data is encrypted and cannot be read without the key
/// - Integrity: Tampering with encrypted data will be detected
/// - Authentication: Only possessors of the key can modify data
///
/// # Performance considerations
///
/// - Chunk size affects both storage efficiency and random access performance
/// - Increasing chunk size improves throughput but reduces random access efficiency
/// - For large objects with frequent random access, consider using smaller chunks
///
/// # Example
/// ```rust,no_run
/// use anda_object_store::EncryptedStoreBuilder;
/// use object_store::memory::InMemory;
///
/// // Create a secret key
/// let secret = [0u8; 32]; // In production, use a secure random key
///
/// // Create an encrypted store with an in-memory backend
/// let store = InMemory::new();
/// let encrypted_store = EncryptedStoreBuilder::with_secret(store, 1000, secret)
///     .build();
/// ```
///
/// # Example 2
/// ```rust,no_run
/// use anda_object_store::EncryptedStoreBuilder;
/// use object_store::local::LocalFileSystem;
///
/// // Create a secret key
/// let secret = [0u8; 32]; // In production, use a secure random key
///
/// // Create an encrypted store with an local file system backend
/// let store = LocalFileSystem::new_with_prefix("my_store").unwrap();
/// let encrypted_store = EncryptedStoreBuilder::with_secret(store, 1000, secret)
///     .with_chunk_size(1024 * 1024) // Set chunk size to 1 MB
///     .with_conditional_put() // Should be enabled for LocalFileSystem
///     .build();
/// ```
#[derive(Clone)]
pub struct EncryptedStore<T: ObjectStore> {
    inner: Arc<EncryptedStoreBuilder<T>>,
}

/// Builder for configuring and creating an `EncryptedStore` instance.
///
/// This struct provides methods to customize the behavior of the encryption store,
/// such as chunk size, conditional put operations, and more.
pub struct EncryptedStoreBuilder<T: ObjectStore> {
    store: T,
    cipher: Arc<Aes256Gcm>,
    chunk_size: u64,
    conditional_put: bool,
    data_prefix: Path,
    meta_prefix: Path,
    meta_cache: Cache<Path, Arc<Metadata>>,
}

/// Metadata structure for storing encryption details.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Metadata {
    #[serde(rename = "s")]
    size: u64,

    #[serde(rename = "e")]
    e_tag: Option<String>,

    #[serde(rename = "o")]
    original_tag: Option<String>,

    /// A nonce with AES256-GCM encryption
    #[serde(rename = "n")]
    aes_nonce: ByteArray<12>,

    /// A set of tags with AES256-GCM encryption
    /// Each part of the object has its own tag
    #[serde(rename = "t")]
    aes_tags: Vec<ByteArray<16>>,
}

impl<T: ObjectStore> std::fmt::Display for EncryptedStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptedStore({:?})", self.inner.store)
    }
}

impl<T: ObjectStore> std::fmt::Debug for EncryptedStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptedStore({:?})", self.inner.store)
    }
}

impl<T: ObjectStore> EncryptedStoreBuilder<T> {
    /// Creates a new `EncryptedStoreBuilder` with the provided secret key.
    ///
    /// This method initializes an AES-256-GCM cipher using the provided secret key.
    ///
    /// # Parameters
    /// - `store`: The underlying object store implementation
    /// - `meta_cache_capacity`: Maximum number of metadata entries to cache
    /// - `secret`: A 32-byte secret key for AES-256-GCM encryption
    ///
    /// # Returns
    /// A new `EncryptedStoreBuilder` instance
    pub fn with_secret(store: T, meta_cache_capacity: u64, secret: [u8; 32]) -> Self {
        use aes_gcm::aead::KeyInit;

        let key = Key::<Aes256Gcm>::from(secret);
        EncryptedStoreBuilder::new(store, meta_cache_capacity, Arc::new(Aes256Gcm::new(&key)))
    }

    /// Creates a new `EncryptedStoreBuilder` with the provided AES-256-GCM cipher.
    ///
    /// This method allows for more flexibility in how the cipher is created.
    ///
    /// # Parameters
    /// - `store`: The underlying object store implementation
    /// - `meta_cache_capacity`: Maximum number of metadata entries to cache
    /// - `cipher`: An AES-256-GCM cipher instance
    ///
    /// # Returns
    /// A new `EncryptedStoreBuilder` instance with default settings
    pub fn new(store: T, meta_cache_capacity: u64, cipher: Arc<Aes256Gcm>) -> Self {
        EncryptedStoreBuilder {
            store,
            cipher,
            chunk_size: 256 * 1024,
            conditional_put: false,
            data_prefix: Path::from("data"),
            meta_prefix: Path::from("meta"),
            meta_cache: Cache::builder()
                .max_capacity(meta_cache_capacity)
                .time_to_live(Duration::from_secs(60 * 60))
                .time_to_idle(Duration::from_secs(20 * 60))
                .build(),
        }
    }

    /// Sets the cache for metadata.
    ///
    /// This cache is used to store metadata for objects, improving performance.
    ///
    /// # Parameters
    /// - `cache`: The cache to use for metadata
    ///
    /// # Returns
    /// The builder with the updated metadata cache
    pub fn with_meta_cache(self, cache: Cache<Path, Arc<Metadata>>) -> Self {
        Self {
            meta_cache: cache,
            ..self
        }
    }

    /// Sets the chunk size for encryption operations.
    ///
    /// Large objects are split into chunks of this size before encryption.
    /// Each chunk is encrypted separately.
    ///
    /// # Parameters
    /// - `chunk_size`: The size of each chunk in bytes, default is 256 KB
    ///
    /// # Returns
    /// The builder with the updated chunk size
    pub fn with_chunk_size(self, chunk_size: u64) -> Self {
        Self { chunk_size, ..self }
    }

    /// Enables conditional put operations.
    ///
    /// When enabled, put operations will check the extend e-tag of the existing object
    /// before overwriting it, providing optimistic concurrency control.
    ///
    /// # Returns
    /// The builder with conditional put enabled
    pub fn with_conditional_put(self) -> Self {
        Self {
            conditional_put: true,
            ..self
        }
    }

    /// Builds and returns an `EncryptedStore` with the configured settings.
    ///
    /// # Returns
    /// A new `EncryptedStore` instance
    pub fn build(self) -> EncryptedStore<T> {
        EncryptedStore {
            inner: Arc::new(self),
        }
    }

    fn meta_path(&self, location: &Path) -> Path {
        self.meta_prefix.parts().chain(location.parts()).collect()
    }

    fn full_path(&self, location: &Path) -> Path {
        self.data_prefix.parts().chain(location.parts()).collect()
    }

    fn strip_prefix(&self, path: Path) -> Path {
        if let Some(suffix) = path.prefix_match(&self.data_prefix) {
            return suffix.collect();
        }
        path
    }

    async fn load_meta(&self, location: &Path) -> Result<Metadata> {
        let meta_path = self.meta_path(location);
        let data = self.store.get(&meta_path).await?;
        let data = data.bytes().await?;
        let meta: Metadata = from_reader(&data[..]).map_err(|err| Error::Generic {
            store: "EncryptedStore",
            source: format!("Failed to deserialize Metadata for path {location}: {err:?}").into(),
        })?;
        Ok(meta)
    }

    async fn get_meta(&self, location: &Path) -> Result<Metadata> {
        let meta = self
            .meta_cache
            .try_get_with(location.clone(), async {
                let meta = self.load_meta(location).await?;
                Ok(Arc::new(meta))
            })
            .await
            .map_err(map_arc_error)?;

        Ok(meta.as_ref().clone())
    }

    async fn put_meta(&self, location: &Path, meta: Metadata) -> Result<PutResult> {
        let meta_path = self.meta_path(location);
        let mut data = Vec::new();
        into_writer(&meta, &mut data).map_err(|err| Error::Generic {
            store: "EncryptedStore",
            source: format!("Failed to serialize Metadata: {err:?}").into(),
        })?;
        self.meta_cache
            .insert(location.clone(), Arc::new(meta))
            .await;
        let rt = self
            .store
            .put_opts(&meta_path, data.into(), PutOptions::default())
            .await?;
        Ok(rt)
    }

    async fn update_meta_with<F>(&self, location: &Path, f: F) -> Result<Arc<Metadata>>
    where
        F: AsyncFnOnce(Option<&Metadata>) -> Result<Metadata>,
    {
        let rt = self
            .meta_cache
            .entry(location.clone())
            .and_try_compute_with(|entry| async {
                let val = match entry {
                    Some(meta) => f(Some(meta.value())).await?,
                    None => match self.load_meta(location).await {
                        Ok(meta) => f(Some(&meta)).await?,
                        Err(Error::NotFound { .. }) => f(None).await?,
                        Err(err) => return Err(err),
                    },
                };

                let meta_path = self.meta_path(location);
                let mut data = Vec::new();
                into_writer(&val, &mut data).map_err(|err| Error::Generic {
                    store: "EncryptedStore",
                    source: format!("Failed to serialize Metadata for path {location}: {err:?}")
                        .into(),
                })?;
                self.store
                    .put_opts(&meta_path, data.into(), PutOptions::default())
                    .await?;
                Ok::<_, Error>(Op::Put(Arc::new(val)))
            })
            .await?;
        Ok(rt.unwrap().value().clone())
    }

    async fn remove_meta_cache(&self, location: &Path) {
        self.meta_cache.remove(location).await;
    }

    async fn get_chunk(&self, location: &Path, idx: u64) -> Result<Vec<u8>> {
        let full_path = self.full_path(location);
        let range = (idx * self.chunk_size)..((idx + 1) * self.chunk_size);
        let data = self.store.get_range(&full_path, range).await?;
        Ok(data.into())
    }
}

pub fn get_range_to_range(
    gr: Option<&GetRange>,
    len: u64,
) -> object_store::Result<Range<u64>, object_store::Error> {
    if gr.is_none() {
        return Ok(0..len);
    }
    let gr = gr.unwrap();
    match gr {
        GetRange::Bounded(r) => {
            if r.start >= len {
                let msg = format!(
                    "Range start {} must not be greater than buffer length {}",
                    r.start, len,
                );
                Err(object_store::Error::Generic {
                    store: "CryptFileSystem",
                    source: msg.into(),
                })
            } else if r.end > len {
                Ok(r.start..len)
            } else {
                Ok(r.start..r.end)
            }
        }
        GetRange::Offset(o) => {
            if *o >= len {
                let msg = format!(
                    "Range start {} must not be greater than buffer length {}",
                    *o, len,
                );
                Err(object_store::Error::Generic {
                    store: "CryptFileSystem",
                    source: msg.into(),
                })
            } else {
                Ok(*o..len)
            }
        }
        GetRange::Suffix(n) => Ok(len.saturating_sub(*n)..len),
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for EncryptedStore<T> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        let rt = self
            .inner
            .update_meta_with(location, async |meta| {
                if self.inner.conditional_put {
                    if let PutMode::Update(v) = &opts.mode {
                        match meta {
                            Some(m) => {
                                if m.e_tag != v.e_tag {
                                    return Err(Error::Precondition {
                                        path: location.to_string(),
                                        source: format!(
                                            "{:?} does not match {:?}",
                                            m.e_tag, v.e_tag
                                        )
                                        .into(),
                                    });
                                }
                            }
                            None => {
                                return Err(Error::Precondition {
                                    path: location.to_string(),
                                    source: "metadata not found".into(),
                                });
                            }
                        }

                        opts.mode = PutMode::Overwrite;
                    }
                }

                let full_path = self.inner.full_path(location);
                let payload = Bytes::from(payload);

                let nonce: [u8; 12] = rand_bytes();
                let mut data: Vec<u8> = payload.into();
                let mut aes_tags: Vec<ByteArray<16>> = Vec::new();
                for chunk in data.chunks_mut(self.inner.chunk_size as usize) {
                    let tag = self
                        .inner
                        .cipher
                        .encrypt_in_place_detached(Nonce::from_slice(&nonce), &[], chunk)
                        .map_err(|err| Error::Generic {
                            store: "EncryptedStore",
                            source: format!("AES256 encrypt failed for path {location}: {err:?}")
                                .into(),
                        })?;
                    let tag: [u8; 16] = tag.into();
                    aes_tags.push(tag.into());
                }

                let hash = sha3_256(&data);
                let mut meta = Metadata {
                    size: data.len() as u64,
                    e_tag: Some(BASE64_URL_SAFE.encode(hash)),
                    original_tag: None,
                    aes_nonce: nonce.into(),
                    aes_tags,
                };

                let rt = self
                    .inner
                    .store
                    .put_opts(&full_path, data.into(), opts)
                    .await?;

                meta.original_tag = rt.e_tag.clone();
                Ok(meta)
            })
            .await?;

        Ok(PutResult {
            e_tag: if self.inner.conditional_put {
                rt.e_tag.clone()
            } else {
                rt.original_tag.clone()
            },
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        let full_path = self.inner.full_path(location);
        let inner = self
            .inner
            .store
            .put_multipart_opts(&full_path, opts)
            .await?;

        Ok(Box::new(EncryptedStoreUploader {
            buf: Vec::new(),
            hasher: sha3::Sha3_256::new(),
            size: 0,
            aes_nonce: rand_bytes(),
            aes_tags: Vec::new(),
            location: location.clone(),
            store: self.inner.clone(),
            inner,
        }))
    }

    async fn get_opts(&self, location: &Path, mut options: GetOptions) -> Result<GetResult> {
        let full_path = self.inner.full_path(location);
        let meta = self.inner.get_meta(location).await?;

        if self.inner.conditional_put {
            if meta.e_tag == options.if_match {
                options.if_match = meta.original_tag.clone();
            }
            if meta.e_tag == options.if_none_match {
                options.if_none_match = meta.original_tag.clone();
            }
        }

        // 原始 range
        let range = if let Some(r) = &options.range {
            get_range_to_range(Some(r), meta.size).map_err(|source| object_store::Error::Generic {
                store: "EncryptedStore",
                source: source.into(),
            })?
                /*
               // as_range is private in old object_store
               r.as_range(meta.size)
                .map_err(|source| object_store::Error::Generic {
                    store: "EncryptedStore",
                    source: source.into(),
                })?
                
                 */
        } else {
            0..meta.size
        };

        // 调整 range，确保读取到包含原始 range 的完整的 chunks，用于解密
        let rr = (range.start / self.inner.chunk_size) * self.inner.chunk_size
            ..meta.size.min(
                (1 + range.end.saturating_sub(1) / self.inner.chunk_size) * self.inner.chunk_size,
            );

        if rr.end > rr.start {
            options.range = Some(GetRange::Bounded(rr.clone()));
        }

        let res = self.inner.store.get_opts(&full_path, options).await?;
        let mut obj = res.meta.clone();
        obj.location = self.inner.strip_prefix(obj.location);
        if self.inner.conditional_put {
            obj.e_tag = meta.e_tag;
        }

        let attributes = res.attributes.clone();

        let chunk_size = self.inner.chunk_size as usize;
        let start_idx = rr.start as usize / chunk_size;
        let start_offset = (range.start - rr.start) as usize;
        let size = (range.end - range.start) as usize;

        let stream = create_decryption_stream(
            res,
            self.inner.cipher.clone(),
            meta.aes_tags.clone(),
            meta.aes_nonce.as_slice().to_vec(),
            location.clone(),
            chunk_size,
            start_idx,
            start_offset,
            size,
        );

        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta: obj,
            range,
            attributes,
        })
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let mut res = self.get_ranges(location, &[range]).await?;
        res.pop().ok_or_else(|| Error::NotFound {
            path: location.as_ref().to_string(),
            source: "get_ranges result should not be empty".into(),
        })
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let meta = self.inner.get_meta(location).await?;
        ranges_is_valid(ranges, meta.size)?;

        let mut result: Vec<Bytes> = Vec::with_capacity(ranges.len());
        let mut chunk_cache: Option<(usize, Vec<u8>)> = None; // cache the last chunk read
        let nonce_ref = Nonce::from_slice(meta.aes_nonce.as_slice());
        for &Range { start, end } in ranges {
            let mut buf = Vec::with_capacity((end - start) as usize);
            // Calculate the chunk indices we need to read
            let start_chunk = (start / self.inner.chunk_size) as usize;
            let end_chunk = ((end - 1) / self.inner.chunk_size) as usize;

            for idx in start_chunk..=end_chunk {
                // Calculate the byte range within this chunk
                let chunk_start = if idx == start_chunk {
                    start % self.inner.chunk_size
                } else {
                    0
                };

                let chunk_end = if idx == end_chunk {
                    (end - 1) % self.inner.chunk_size + 1
                } else {
                    self.inner.chunk_size
                };

                match &chunk_cache {
                    Some((cached_idx, cached_chunk)) if *cached_idx == idx => {
                        buf.extend_from_slice(
                            &cached_chunk[chunk_start as usize..chunk_end as usize],
                        );
                    }
                    _ => {
                        let tag = meta.aes_tags.get(idx).ok_or_else(|| Error::Generic {
                            store: "EncryptedStore",
                            source: format!(
                                "missing AES256 tag for chunk {idx} for path {location}"
                            )
                            .into(),
                        })?;
                        let mut chunk = self.inner.get_chunk(location, idx as u64).await?;
                        self.inner
                            .cipher
                            .decrypt_in_place_detached(
                                nonce_ref,
                                &[],
                                &mut chunk,
                                Tag::from_slice(tag.as_slice()),
                            )
                            .map_err(|err| Error::Generic {
                                store: "EncryptedStore",
                                source: format!(
                                    "AES256 decrypt failed for path {location}: {err:?}"
                                )
                                .into(),
                            })?;
                        buf.extend_from_slice(&chunk[chunk_start as usize..chunk_end as usize]);
                        chunk_cache = Some((idx, chunk));
                    }
                }
            }
            result.push(buf.into());
        }

        Ok(result)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let full_path = self.inner.full_path(location);
        let mut obj = self.inner.store.head(&full_path).await?;
        obj.location = self.inner.strip_prefix(obj.location);

        if self.inner.conditional_put {
            let meta = self.inner.get_meta(&obj.location).await?;
            obj.e_tag = meta.e_tag;
        }

        Ok(obj)
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let full_path = self.inner.full_path(location);
        self.inner.store.delete(&full_path).await?;

        let meta_path = self.inner.meta_path(location);
        self.inner.store.delete(&meta_path).await?;
        self.inner.remove_meta_cache(location).await;
        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = self.inner.full_path(prefix.unwrap_or(&Path::default()));
        let stream = self.inner.store.list(Some(&prefix));

        let inner = self.inner.clone();
        if !self.inner.conditional_put {
            return stream
                .map_ok(move |mut obj| {
                    obj.location = inner.strip_prefix(obj.location);
                    obj
                })
                .boxed();
        }

        stream
            .and_then(move |mut obj| {
                let store = inner.clone();
                async move {
                    let location = store.strip_prefix(obj.location);
                    let meta = store.get_meta(&location).await?;
                    obj.location = location;
                    obj.e_tag = meta.e_tag;
                    Ok(obj)
                }
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = self.inner.full_path(offset);
        let prefix = self.inner.full_path(prefix.unwrap_or(&Path::default()));
        let stream = self.inner.store.list_with_offset(Some(&prefix), &offset);

        let inner = self.inner.clone();
        if !self.inner.conditional_put {
            return stream
                .map_ok(move |mut obj| {
                    obj.location = inner.strip_prefix(obj.location);
                    obj
                })
                .boxed();
        }

        stream
            .and_then(move |mut obj| {
                let store = inner.clone();
                async move {
                    let location = store.strip_prefix(obj.location);
                    let meta = store.get_meta(&location).await?;
                    obj.location = location;
                    obj.e_tag = meta.e_tag;
                    Ok(obj)
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = self.inner.full_path(prefix.unwrap_or(&Path::default()));
        let rt = self.inner.store.list_with_delimiter(Some(&prefix)).await?;
        let mut rt = ListResult {
            common_prefixes: rt
                .common_prefixes
                .into_iter()
                .map(|p| self.inner.strip_prefix(p))
                .collect(),
            objects: rt
                .objects
                .into_iter()
                .map(|mut meta| {
                    meta.location = self.inner.strip_prefix(meta.location);
                    meta
                })
                .collect(),
        };

        if self.inner.conditional_put {
            for obj in rt.objects.iter_mut() {
                let meta = self.inner.get_meta(&obj.location).await?;
                obj.e_tag = meta.e_tag;
            }
        }

        Ok(rt)
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.inner.full_path(from);
        let full_to = self.inner.full_path(to);
        self.inner.store.copy(&full_from, &full_to).await?;

        let meta_from = self.inner.meta_path(from);
        let meta_to = self.inner.meta_path(to);
        self.inner.store.copy(&meta_from, &meta_to).await?;
        self.inner.remove_meta_cache(to).await;
        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.inner.full_path(from);
        let full_to = self.inner.full_path(to);
        self.inner.store.rename(&full_from, &full_to).await?;
        self.inner.remove_meta_cache(from).await;

        let meta_from = self.inner.meta_path(from);
        let meta_to = self.inner.meta_path(to);
        self.inner.store.rename(&meta_from, &meta_to).await?;
        self.inner.remove_meta_cache(to).await;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.inner.full_path(from);
        let full_to = self.inner.full_path(to);
        self.inner
            .store
            .copy_if_not_exists(&full_from, &full_to)
            .await?;

        let meta_from = self.inner.meta_path(from);
        let meta_to = self.inner.meta_path(to);
        self.inner
            .store
            .copy_if_not_exists(&meta_from, &meta_to)
            .await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.inner.full_path(from);
        let full_to = self.inner.full_path(to);
        self.inner
            .store
            .rename_if_not_exists(&full_from, &full_to)
            .await?;
        self.inner.remove_meta_cache(from).await;

        let meta_from = self.inner.meta_path(from);
        let meta_to = self.inner.meta_path(to);
        self.inner
            .store
            .rename_if_not_exists(&meta_from, &meta_to)
            .await
    }
}

pub struct EncryptedStoreUploader<T: ObjectStore> {
    buf: Vec<u8>,
    /// Hasher for calculating the content hash
    hasher: sha3::Sha3_256,
    /// Total size of the uploaded content
    size: usize,
    aes_tags: Vec<ByteArray<16>>,
    aes_nonce: [u8; 12],
    /// Logical path of the object
    location: Path,
    /// Reference to the MetaStoreBuilder
    store: Arc<EncryptedStoreBuilder<T>>,
    /// Underlying multipart upload handler
    inner: Box<dyn MultipartUpload>,
}

impl<T: ObjectStore> std::fmt::Debug for EncryptedStoreUploader<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EncryptedStoreUploader({})", self.location)
    }
}

#[async_trait]
impl<T: ObjectStore> MultipartUpload for EncryptedStoreUploader<T> {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let payload = Bytes::from(payload);
        self.size += payload.len();
        self.buf.extend_from_slice(&payload);
        if self.buf.len() < self.store.chunk_size as usize {
            return Box::pin(futures::future::ready(Ok(())));
        }

        let mut parts: Vec<UploadPart> = Vec::new();
        let nonce = Nonce::from_slice(&self.aes_nonce);
        while self.buf.len() >= self.store.chunk_size as usize {
            let mut chunk = self
                .buf
                .drain(..self.store.chunk_size as usize)
                .collect::<Vec<u8>>();

            match self
                .store
                .cipher
                .encrypt_in_place_detached(nonce, &[], &mut chunk)
            {
                Ok(tag) => {
                    let tag: [u8; 16] = tag.into();
                    self.aes_tags.push(tag.into());
                }
                Err(err) => {
                    return Box::pin(futures::future::ready(Err(Error::Generic {
                        store: "EncryptedStore",
                        source: format!(
                            "AES256 encrypt failed for path {}: {err:?}",
                            self.location
                        )
                        .into(),
                    })));
                }
            }
            self.hasher.update(&chunk);
            parts.push(self.inner.put_part(chunk.into()))
        }

        Box::pin(async move {
            for part in parts {
                part.await?;
            }

            Ok(())
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let nonce = Nonce::from_slice(&self.aes_nonce);
        for chunk in self.buf.chunks_mut(self.store.chunk_size as usize) {
            let tag = self
                .store
                .cipher
                .encrypt_in_place_detached(nonce, &[], chunk)
                .map_err(|err| Error::Generic {
                    store: "EncryptedStore",
                    source: format!("AES256 encrypt failed for path {}: {err:?}", self.location)
                        .into(),
                })?;
            let tag: [u8; 16] = tag.into();
            self.aes_tags.push(tag.into());
            self.hasher.update(&chunk);
            self.inner.put_part(chunk.to_vec().into()).await?;
        }

        self.buf.clear();
        let hash: [u8; 32] = self.hasher.clone().finalize().into();
        let mut rt = self.inner.complete().await?;
        let obj = self
            .store
            .store
            .head(&self.store.full_path(&self.location))
            .await?;

        let meta = Metadata {
            size: self.size as u64,
            e_tag: Some(BASE64_URL_SAFE.encode(hash)),
            original_tag: obj.e_tag,
            aes_nonce: self.aes_nonce.into(),
            aes_tags: self.aes_tags.clone(),
        };

        if self.store.conditional_put {
            rt.e_tag = meta.e_tag.clone();
        }
        self.store.put_meta(&self.location, meta).await?;
        Ok(rt)
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[allow(clippy::too_many_arguments)]
fn create_decryption_stream(
    res: GetResult,
    cipher: Arc<Aes256Gcm>,
    aes_tags: Vec<ByteArray<16>>,
    nonce: Vec<u8>,
    location: Path,
    chunk_size: usize,
    start_idx: usize,
    start_offset: usize,
    size: usize,
) -> BoxStream<'static, Result<Bytes>> {
    try_stream! {
        let mut stream = res.into_stream();
        // 预分配足够大的缓冲区以减少重新分配次数
        let mut buf = Vec::with_capacity(chunk_size * 2);
        let mut idx = start_idx;
        let mut remaining = size;
        let nonce_ref = Nonce::from_slice(&nonce);

        while let Some(data) = stream.next().await {
            let data = data?;
            buf.extend_from_slice(&data);

            while buf.len() > chunk_size {
                let mut chunk = buf.drain(..chunk_size).collect::<Vec<u8>>();

                let tag = aes_tags.get(idx).ok_or_else(|| Error::Generic {
                    store: "EncryptedStore",
                    source: format!("missing AES256 tag for chunk {idx} for path {location}").into(),
                })?;

                cipher.decrypt_in_place_detached(
                    nonce_ref,
                    &[],
                    &mut chunk,
                    Tag::from_slice(tag.as_slice())
                )
                .map_err(|err| Error::Generic {
                    store: "EncryptedStore",
                    source: format!("AES256 decrypt failed for path {location}: {err:?}").into(),
                })?;

                if idx == start_idx && start_offset > 0 {
                    chunk.drain(..start_offset);
                }

                remaining = remaining.saturating_sub(chunk.len());
                yield Bytes::from(chunk);

                idx += 1;
            }
        }

        if !buf.is_empty() {
            let tag = aes_tags.get(idx).ok_or_else(|| Error::Generic {
                store: "EncryptedStore",
                source: format!("missing AES256 tag for chunk {idx} for path {location}").into(),
            })?;
            cipher.decrypt_in_place_detached(
                nonce_ref,
                &[],
                &mut buf,
                Tag::from_slice(tag.as_slice())
            )
            .map_err(|err| Error::Generic {
                store: "EncryptedStore",
                source: format!("AES256 decrypt failed for path {location}: {err:?}").into(),
            })?;

            if idx == start_idx && start_offset > 0 {
                buf.drain(..start_offset);
            }

            buf.truncate(remaining);
            yield Bytes::from(buf);
        }
    }.boxed()
}

fn ranges_is_valid(ranges: &[Range<u64>], len: u64) -> Result<()> {
    for range in ranges {
        if range.start >= len {
            return Err(Error::Generic {
                store: "EncryptedStore",
                source: format!("start {} is larger than length {}", range.start, len).into(),
            });
        }
        if range.end <= range.start {
            return Err(Error::Generic {
                store: "EncryptedStore",
                source: format!("end {} is less than start {}", range.end, range.start).into(),
            });
        }
    }
    Ok(())
}

fn rand_bytes<const N: usize>() -> [u8; N] {
    let mut rng = rand::rng();
    let mut bytes = [0u8; N];
    rng.fill_bytes(&mut bytes);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{integration::*, local::LocalFileSystem, memory::InMemory};
    use tempfile::TempDir;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[tokio::test]
    async fn test_with_memory() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 10000, [0u8; 32]).build();

        let location = Path::from(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&storage, Some(location))
            .await
            .unwrap_err();
        if let crate::Error::NotFound { path, .. } = err {
            assert!(path.ends_with(NON_EXISTENT_NAME));
        } else {
            panic!("unexpected error type: {err:?}");
        }

        put_get_delete_list(&storage).await;
        put_get_attributes(&storage).await;
        get_opts(&storage).await;
        put_opts(&storage, true).await;

        list_uses_directories_correctly(&storage).await;
        list_with_delimiter(&storage).await;
        rename_and_copy(&storage).await;
        copy_if_not_exists(&storage).await;
        copy_rename_nonexistent_object(&storage).await;
        multipart_race_condition(&storage, true).await;
        multipart_out_of_order(&storage).await;

        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 10000, [0u8; 32]).build();
        stream_get(&storage).await;
    }

    #[tokio::test]
    async fn test_with_memory_conditional_put() {
        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 10000, [0u8; 32])
            .with_conditional_put()
            .build();

        let location = Path::from(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&storage, Some(location))
            .await
            .unwrap_err();
        if let crate::Error::NotFound { path, .. } = err {
            assert!(path.ends_with(NON_EXISTENT_NAME));
        } else {
            panic!("unexpected error type: {err:?}");
        }

        put_get_delete_list(&storage).await;
        put_get_attributes(&storage).await;
        get_opts(&storage).await;
        put_opts(&storage, true).await;

        list_uses_directories_correctly(&storage).await;
        list_with_delimiter(&storage).await;
        rename_and_copy(&storage).await;
        copy_if_not_exists(&storage).await;
        copy_rename_nonexistent_object(&storage).await;
        multipart_race_condition(&storage, true).await;
        multipart_out_of_order(&storage).await;

        let storage = EncryptedStoreBuilder::with_secret(InMemory::new(), 10000, [0u8; 32])
            .with_conditional_put()
            .build();
        stream_get(&storage).await;
    }

    #[tokio::test]
    async fn test_with_local_file() {
        let root = TempDir::new().unwrap();
        let storage = EncryptedStoreBuilder::with_secret(
            LocalFileSystem::new_with_prefix(root.path()).unwrap(),
            10000,
            [0u8; 32],
        )
        .with_conditional_put()
        .build();

        let location = Path::from(NON_EXISTENT_NAME);

        let err = get_nonexistent_object(&storage, Some(location))
            .await
            .unwrap_err();
        if let crate::Error::NotFound { path, .. } = err {
            assert!(path.ends_with(NON_EXISTENT_NAME));
        } else {
            panic!("unexpected error type: {err:?}");
        }

        put_get_delete_list(&storage).await;
        put_get_attributes(&storage).await;
        get_opts(&storage).await;
        put_opts(&storage, true).await;

        list_uses_directories_correctly(&storage).await;
        list_with_delimiter(&storage).await;
        rename_and_copy(&storage).await;
        copy_if_not_exists(&storage).await;
        copy_rename_nonexistent_object(&storage).await;
        multipart_race_condition(&storage, true).await;
        multipart_out_of_order(&storage).await;

        let root = TempDir::new().unwrap();
        let storage = EncryptedStoreBuilder::with_secret(
            LocalFileSystem::new_with_prefix(root.path()).unwrap(),
            10000,
            [0u8; 32],
        )
        .with_conditional_put()
        .build();
        stream_get(&storage).await;
    }
}
