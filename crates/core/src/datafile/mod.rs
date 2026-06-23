//! Consolidated data-file read/write abstractions.
//!
//! These abstractions are organized into two tiers so that advanced parquet
//! concerns — most importantly **encryption/decryption** — have a single,
//! well-defined home: the per-file boundary, where parquet `WriterProperties` /
//! `FileEncryptionProperties` actually attach.
//!
//! * **File tier** ([`DataFileWriter`], [`reader::DataFileReader`]) — the
//!   per-file seam. A [`DataFileWriter`] owns the underlying parquet writer, so
//!   encryption (and any other parquet-IO property) is configured here, once.
//!   Its implementation is [`writer::PartitionWriter`].
//! * **Dataset tier** ([`DeltaDataWriter`], [`DeltaDataReader`]) — composes the
//!   file tier across a whole table: partitioning a stream of batches into many
//!   files on write, and many files into one stream on read. Its writer
//!   implementation is [`writer::DeltaWriter`].
//!
//! Every production write flows through a [`DataFileWriter`]: the dataset writer
//! composes it, table-compaction (`optimize`) uses it directly, and the legacy
//! `RecordBatchWriter`/`JsonWriter` go through the dataset writer. That single
//! seam is what lets us set encryption in one place.
//!
//! Both tiers operate on the same DataFusion-free "narrow waist": a stream of
//! futures, each yielding an Arrow [`RecordBatch`]. The DataFusion-capable
//! surface lives in the gated [`ext`] module ([`ext::DeltaDataWriterExt`],
//! [`ext::DeltaDataReaderExt`]) and late-materializes a `DataFrame`/`ExecutionPlan`
//! into that waist before delegating to the basic implementation.

use arrow_array::RecordBatch;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt as _};

use crate::errors::DeltaResult;
use crate::kernel::Add;

pub mod reader;
pub mod writer;

#[cfg(feature = "datafusion")]
pub mod ext;

/// A future that resolves to a single [`RecordBatch`] (or an error).
///
/// This is the unit of late materialization: producers can hand back work that
/// has not been executed yet, letting the consumer drive it with bounded
/// concurrency.
pub type BatchFuture = BoxFuture<'static, DeltaResult<RecordBatch>>;

/// The narrow waist: a stream of [`BatchFuture`]s.
///
/// Draining it with bounded concurrency (e.g. [`futures::StreamExt::buffered`])
/// yields parallel data-file reads and writes.
pub type RecordBatchFutureStream = BoxStream<'static, BatchFuture>;

/// Build a [`RecordBatchFutureStream`] from already-materialized record batches.
///
/// Each batch is wrapped in a ready future, so draining the stream simply yields
/// the batches in order; useful for feeding buffered batches into a writer.
pub fn batches_to_future_stream(batches: Vec<RecordBatch>) -> RecordBatchFutureStream {
    futures::stream::iter(
        batches
            .into_iter()
            .map(|batch| -> BatchFuture { Box::pin(async move { Ok(batch) }) }),
    )
    .boxed()
}

/// File tier: a writer for a single Delta data file (or a small set of size-split
/// files for one partition).
///
/// This is the per-file seam where parquet `WriterProperties` — including, in the
/// future, `FileEncryptionProperties` — attach. Its implementation,
/// [`writer::PartitionWriter`], owns the underlying parquet writer. The dataset
/// writer ([`DeltaWriter`]) composes one of these per partition; table compaction
/// (`optimize`) uses one directly. Setting encryption on the configuration handed
/// to the implementation therefore covers every production write.
#[async_trait::async_trait]
pub trait DataFileWriter: Send {
    /// Buffer a record batch, writing to one or more parquet files as needed.
    /// The batch must match the writer's (partition-stripped) file schema.
    async fn write(&mut self, batch: &RecordBatch) -> DeltaResult<()>;

    /// Finish writing and return the uncommitted [`Add`] actions for the files
    /// that were produced.
    async fn close(self: Box<Self>) -> DeltaResult<Vec<Add>>;
}

/// Options controlling a basic (DataFusion-free) read.
///
/// Richer predicate/projection pushdown is the responsibility of the
/// DataFusion extension trait ([`ext::DeltaDataReaderExt`]); the basic reader
/// only supports column projection and a row limit, plus the file skipping that
/// `delta-kernel` performs from log statistics.
#[derive(Debug, Default, Clone)]
pub struct ReadOptions {
    /// Project to this subset of (logical) column names. `None` reads all columns.
    pub projection: Option<Vec<String>>,
    /// Stop after returning at least this many rows. `None` reads the whole table.
    pub limit: Option<usize>,
}

impl ReadOptions {
    /// Project to the given logical column names.
    pub fn with_projection(mut self, projection: impl Into<Vec<String>>) -> Self {
        self.projection = Some(projection.into());
        self
    }

    /// Limit the number of rows returned.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// Dataset tier: a DataFusion-free writer that consumes a stream of record
/// batches and produces the data files for a whole table write.
///
/// The implementation ([`writer::DeltaWriter`]) partitions the stream by the
/// table's partition columns and composes a [`DataFileWriter`] per partition, so
/// all of its files inherit whatever parquet/encryption properties the file tier
/// is configured with.
///
/// Batches handed to [`write_all`](DeltaDataWriter::write_all) must already
/// conform to the table schema and satisfy any table constraints / invariants /
/// generated-column expressions. In the DataFusion write path that validation is
/// performed upstream as an `ExecutionPlan` node; callers using the basic path
/// directly are responsible for their own validation.
#[async_trait::async_trait]
pub trait DeltaDataWriter: Send {
    /// Drain the batch-future stream into parquet data files, returning the
    /// uncommitted [`Add`] actions. The returned actions still need to be
    /// committed via a transaction.
    async fn write_all(self: Box<Self>, batches: RecordBatchFutureStream) -> DeltaResult<Vec<Add>>;
}

/// Dataset tier: a DataFusion-free reader that produces record batches for a
/// whole table read.
///
/// Implementations compose the file tier ([`reader::DataFileReader`]) across the
/// table's data files and apply deletion vectors, partition value injection, and
/// column-mapping transforms so the emitted batches are in the table's logical
/// schema.
#[async_trait::async_trait]
pub trait DeltaDataReader: Send + Sync {
    /// Read the selected data as a stream of batch futures.
    async fn read(&self, options: ReadOptions) -> DeltaResult<RecordBatchFutureStream>;
}
