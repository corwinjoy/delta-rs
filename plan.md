# FileFormatOptions — Minimal Encryption Implementation

## Why this branch exists

PR #2 (`file-format-options-rebase`) added `FileFormatOptions` and KMS-based Parquet
encryption to delta-rs but was rejected upstream because the diff touched ~30 files
and ~2000 lines.  The core complaint was that threading a `WriterPropertiesFactory`
through every operation builder (delete, update, merge, optimize, write) caused the
change to be too large to review.

This branch reimplements the same feature with a smaller, more focused diff by using
the **DataFusion session as the carrier for the `WriterPropertiesFactory`** rather than
threading it through every operation's builder struct and function signature.

---

## Key design insight

On `main`, all write execution functions accept `writer_properties: Option<WriterProperties>`
(a static parquet config).  The original PR changed these to `Option<WriterPropertiesFactoryRef>`,
propagating the change through every caller.

The minimal approach instead:

1. **Stores `FileFormatOptions` in `DeltaTableConfig`** — the single source of truth.
2. **`apply_file_format_to_state(SessionState, Option<&FileFormatRef>) → SessionState`**
   registers:
   - The **encryption factory** in the session's `RuntimeEnv` (for reads — exactly like
     the original PR).
   - The **`WriterPropertiesFactory`** as a `DeltaWriterExtension` inside the session's
     `ConfigOptions` (for writes — new to this branch).
3. **Operation builders** (`delete`, `update`, `merge`, `optimize`, `write`) store
   `DeltaTableConfig` via a new `with_table_config()` method and call
   `apply_file_format_to_state` after resolving their DataFusion session — **no type
   changes** to their existing `writer_properties: Option<WriterProperties>` fields.
4. **`write_data_plan` / `write_exec_plan`** retrieve the factory from the session
   extension via `factory_from_session(session)` rather than accepting it as a parameter.
   All write function signatures are **unchanged from `main`**.
5. **`DeltaTable` methods** (`write`, `delete`, `update`, `merge`, `optimize`) pass
   `self.config` to each builder via `.with_table_config(self.config.clone())`.

---

## What changed vs. the original PR

| Original PR | This branch |
|-------------|-------------|
| `writer_properties` field type changed in 5 builder structs | **No type changes** to builder structs |
| Factory initialized from snapshot in each `new()` | `DeltaTableConfig` passed via `with_table_config()` |
| Factory threaded through `execute()` signatures | Factory retrieved from session extension |
| `resolve_snapshot_with_config` added to kernel | **Not needed** — config passed alongside snapshot |
| 5 operation builders × ~40 lines each | 5 operation builders × ~15 lines each |

---

## Read path (how scans decrypt files)

1. `DeltaScanConfigBuilder::build(snapshot)` sets `DeltaScanConfig.table_parquet_options`
   from `snapshot.load_config().file_format_options.table_options().parquet`.  This is
   the only place the scan config learns about encryption.
2. `DeltaScan::scan()` (next provider) calls `ffo.update_session(session)` to register
   the encryption factory in `RuntimeEnv`.
3. `get_read_plan()` in `scan/mod.rs` looks up the factory via `factory_id` and calls
   `.with_encryption_factory(factory)` on the `ParquetSource`.  The cached reader factory
   is **bypassed when encryption is active** (it interferes with decryption).
4. For optimize Z-order and compact reads, `DeltaScanNext` is used directly with a
   `FileSelection` built from the Add actions, ensuring the same encryption path applies.

### `SimpleFileFormatOptions` — static key encryption

Static-key encryption (`SimpleFileFormatOptions`) uses a `StaticEncryptionFactory` that
returns the same `FileDecryptionProperties` for every file.  It is registered via
`factory_id` in the `RuntimeEnv` (the `ConfigFileDecryptionProperties` → `FileDecryptionProperties`
round-trip in the `ParquetSource` opener is unreliable in DataFusion v52, so we bypass it).

---

## Write path (how files are encrypted)

1. `apply_file_format_to_state` stores `WriterPropertiesFactory` as a `DeltaWriterExtension`
   in the session config.
2. `resolve_writer_factory(session, explicit_writer_properties)` checks:
   - Explicit user-set `WriterProperties` (from `with_writer_properties()`) → static factory
   - Session extension factory (from `FileFormatOptions`) → may be async/KMS
   - Default: session parquet config (existing behaviour)
3. `WriterConfig` / `PartitionWriterConfig` / `LazyArrowWriter` use `WriterPropertiesFactory`
   instead of `WriterProperties` so each file can get a unique key (required for KMS).

---

## Files changed

| File | What changed |
|------|-------------|
| `table/file_format_options.rs` | **New file** — all core abstractions |
| `table/mod.rs` | Expose `file_format_options` module |
| `table/builder.rs` | `DeltaTableConfig.file_format_options`, `DeltaTableBuilder::with_file_format_options`, `with_table_config` |
| `operations/mod.rs` | Pass `self.config` to builders; update `DeltaTable` + `DeltaOps` methods |
| `operations/create.rs` | Thread `table_config` so returned table preserves `file_format_options` |
| `operations/delete.rs` | `table_config` field + `apply_file_format_to_state` |
| `operations/update.rs` | `table_config` field + `apply_file_format_to_state` |
| `operations/merge/mod.rs` | `table_config` field + `apply_file_format_to_state` |
| `operations/optimize.rs` | `table_config`, `apply_file_format_to_state`, Z-order + compact via `DeltaScanNext` |
| `operations/write/mod.rs` | `table_config` field + `apply_file_format_to_state` |
| `operations/write/writer.rs` | `WriterConfig`/`PartitionWriterConfig`/`LazyArrowWriter` use factory |
| `operations/write/execution.rs` | `resolve_writer_factory` retrieves from session |
| `operations/write/plan.rs` | Minor struct updates |
| `delta_datafusion/mod.rs` | Re-export `FileSelection` |
| `delta_datafusion/table_provider.rs` | `DeltaScanConfig.table_parquet_options` + ConfigBuilder + old scan encryption |
| `delta_datafusion/table_provider/next/mod.rs` | `update_session` call in `scan()` |
| `delta_datafusion/table_provider/next/scan/mod.rs` | Encryption factory lookup; bypass cached reader |
| `writer/utils.rs` | `next_data_path` takes `Compression` directly |
| `test_utils/kms_encryption.rs` | **New file** — KMS test utilities |
| `tests/commands_with_encryption.rs` | **New file** — integration tests |
| `crates/deltalake/examples/basic_operations_encryption.rs` | **New file** — usage example |

---

## How to verify

```bash
# Build
cargo build -p deltalake-core --features datafusion

# Unit + integration tests
cargo test -p deltalake-core --features datafusion

# End-to-end example (plain + KMS encryption, Z-order, compact, update, delete, merge)
cargo run --example basic_operations_encryption \
  --features "datafusion integration-test" -p deltalake
```
