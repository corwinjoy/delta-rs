# FileFormatOptions — Encryption Implementation: Status and Re-evaluation

## What was actually built (this branch)

This branch (`file-format-options-minimal`) implements per-table Parquet encryption,
including KMS-based per-file key derivation (AAD support).

### Approach taken

- New `FileFormatOptions` trait + async `WriterPropertiesFactory` in `file_format_options.rs`
- `DeltaTableConfig.file_format_options` carries the config
- `apply_file_format_to_state()` registers the encryption factory in DataFusion's `RuntimeEnv`
  AND stores the writer factory as a `DeltaWriterExtension` in the session config
- Operations read `file_format_options` from the resolved snapshot and call
  `apply_file_format_to_state` once in their `IntoFuture`
- Write functions retrieve the factory from the session extension
- `json.rs` and `record_batch.rs` compute the file path before creating the writer
  so the factory can use it for AAD key derivation

### Diff vs main

```
26 files changed, 2180 insertions(+), 147 deletions(-)
```

Excluding tests, example, plan.md (which any approach needs):

```
~15 core files changed, ~1300 lines
```

---

## Honest comparison with PR2 (`file-format-options-rebase`)

PR2 had ~30 files changed, ~2000 insertions. The "minimal" branch ended up with:
- **26 files** — saved 4 files (no `resolve_snapshot_with_config`, no `kernel/snapshot`,
  smaller `operations/mod.rs` changes)
- **2180 insertions** — actually MORE than PR2

**We are not significantly simpler.** The savings in one area (threading factory through
builders) were offset by additions in other areas (AAD support in `json.rs` and
`record_batch.rs`, session extension machinery, `StaticEncryptionFactory` for DataFusion
v52 compatibility). The session-extension pattern hides complexity from callers but adds
hidden state — arguably harder to reason about than the explicit threading in PR2.

### Where the complexity is unavoidable

The fundamental complexity drivers are:

1. **Async per-file key derivation** (KMS) — requires `WriterPropertiesFactory` trait.
   This is not optional for KMS; the key is fetched remotely per file.

2. **Multiple write paths** — delta-rs has at least 7 separate write paths:
   `WriteBuilder`, `DeleteBuilder`, `UpdateBuilder`, `MergeBuilder`, `OptimizeBuilder`
   (compact + z-order), `JsonWriter`, `RecordBatchWriter`. Any approach must touch all of them.

3. **Multiple read paths** — the next provider (`DeltaScanNext`), the old provider
   (`DeltaTableProvider`), and optimize's internal scan.

4. **DataFusion version quirk** — the `ConfigFileDecryptionProperties`→`FileDecryptionProperties`
   round-trip is broken in DataFusion v52, requiring `StaticEncryptionFactory` workaround.

---

## Proposed new design: split into two focused PRs

The core problem is that we're conflating two different use cases in one PR:
- **Static key encryption** (simple, synchronous)
- **KMS dynamic encryption** (complex, async, per-file)

### PR A: Static key encryption (~10 files, ~400 lines)

This covers the majority of users who just want to encrypt files with a fixed key.

Design:
- Add `DeltaTableConfig.writer_properties: Option<WriterProperties>` (not a factory, just
  static properties)
- Operations pass `writer_properties` to `WriterConfig` the same way they already pass it
  via `with_writer_properties()` — but now read from snapshot config automatically
- Reads: store `file_decryption` in `DeltaScanConfig.table_parquet_options` — already exists
  in the DataFusion parquet source and works for static keys
- No async factory, no session extensions, no `FileFormatOptions` trait

Files changed:
- `table/builder.rs` — add field (~5 lines)
- `delta_datafusion/table_provider.rs` — set `table_parquet_options` from snapshot (~15 lines)
- `delta_datafusion/table_provider/next/mod.rs` — apply decryption options (~5 lines)
- `delta_datafusion/table_provider/next/scan/mod.rs` — factory lookup for read (~10 lines)
- `operations/delete.rs`, `update.rs`, `merge/mod.rs`, `write/mod.rs`, `optimize.rs` —
  read `writer_properties` from snapshot, pass to `WriterConfig` (~5 lines each)
- `operations/write/writer.rs` — minimal: `WriterConfig` keeps `WriterProperties` field,
  `LazyArrowWriter` just uses it directly (no factory change needed)

**Total: ~10 files, ~150 lines of real change**

Limitation: same key used for every file — no KMS.

### PR B: KMS dynamic encryption (builds on PR A)

Adds the factory abstraction for per-file key derivation.

Design:
- `FileFormatOptions` trait with `writer_properties_factory()` and `update_session()`
- `WriterPropertiesFactory` async trait
- `WriterConfig` upgrades from `WriterProperties` to `WriterPropertiesFactoryRef`
- Session extension carries factory

This is essentially the current design, but on a smaller base because PR A
already handled the static case. Only the KMS-specific changes land here.

**Total: ~15 additional files, ~700 additional lines**

### Why this split is better

- PR A can ship quickly and gives 90% of users what they need
- PR A is small enough to be reviewed in a single sitting
- PR B is clearly motivated ("here's why we need the factory for KMS") and
  builds obviously on PR A
- If PR B is still rejected as too complex, PR A still ships value

---

## Verdict on the current branch

The current approach is technically correct, well-tested, and passes all CI checks.
The diff is unavoidably large because the feature genuinely touches many files.

If upstream reviewers are willing to accept a single-PR approach, this branch is ready.

If not, the recommended path is to split as described above: PR A (static keys) first,
PR B (KMS factory) second.

---

## Files changed (current branch vs main)

| Category | Files | What |
|----------|-------|------|
| New abstractions | `file_format_options.rs` | FileFormatOptions, WriterPropertiesFactory, StaticEncryptionFactory, session extension |
| Config plumbing | `table/builder.rs`, `table/mod.rs` | DeltaTableConfig.file_format_options |
| Write path | `write/writer.rs`, `write/execution.rs` | Factory-based WriterConfig, resolve_writer_factory |
| Read path | `table_provider.rs`, `next/mod.rs`, `next/scan/mod.rs` | DeltaScanConfig.table_parquet_options, factory lookup |
| Operations (×5) | `delete`, `update`, `merge`, `write/mod`, `optimize` | ~5 lines each: apply_file_format_to_state |
| Create | `create.rs`, `mod.rs` | Preserve file_format_options in returned table |
| Legacy writers | `json.rs`, `record_batch.rs` | Factory + path-based for AAD support |
| Test utilities | `kms_encryption.rs`, `mod.rs` | MockKmsClient, TableEncryption, KmsFileFormatOptions |
| Tests | `commands_with_encryption.rs` | Integration tests for all operations |
| Example | `basic_operations_encryption.rs` | End-to-end usage example |
| Cargo | `Cargo.toml` ×3 | parquet/datafusion encryption features |
| Documentation | `plan.md` | This file |
