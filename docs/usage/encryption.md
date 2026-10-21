# Encrypting a table

delta-rs can encrypt a table's Parquet data files with
[Parquet Modular Encryption](https://parquet.apache.org/docs/file-format/data-pages/encryption/),
following the Delta protocol RFC for
[Parquet encryption](https://github.com/delta-io/delta/issues/6195).
Encryption is configured with table properties when the table is created.

Support is an opt-in cargo feature (`encryption`) of the Rust crates. Builds without it refuse
to read or write encrypted tables, so they never return ciphertext or add plaintext files.

## Table properties

| Property                             | Meaning                                                                                                   |
|--------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `delta.encryption.kms_id`            | Which registered key management system (KMS) client to use. Required.                                     |
| `delta.encryption.kms_configuration` | Opaque, KMS-specific configuration, for example JSON. Optional; may change at any time.                   |
| `delta.encryption.footer_key`        | Master key ID for the footer. Setting it turns encryption on.                                             |
| `delta.encryption.plaintext_footer`  | `true` leaves the footer readable but signed with the footer key. Default `false`.                        |
| `delta.encryption.column_keys`       | Columns to encrypt with their master key IDs, as `keyId:col1,col2;keyId2:col3`. Empty encrypts every column with the footer key. |

The properties are plaintext in the Delta log, so they hold only key IDs and KMS settings,
never keys or credentials. Key material for each data file lives in the file's own Parquet
key metadata.

`kms_configuration` is also writable by anyone who can commit to the table, so a KMS client
must treat it as untrusted input. A client that takes its endpoint from it should connect only
to allowlisted endpoints; otherwise whoever edits the table can redirect key requests, and the
credentials sent with them, to a server of their choosing.

`footer_key` is always required: Parquet encrypts the footer with it, or signs the footer with
it under `plaintext_footer`. There is no footer-less mode. An encrypted footer hides which
columns are encrypted and the Parquet-level statistics. This is stricter than the RFC, which
lets a table keep `kms_id` and `kms_configuration` after its footer key was unset, meaning
encryption was turned off; delta-rs refuses such a table rather than treat it as plaintext.

Column names in `column_keys` may be dot-separated to name a struct field, which encrypts every
field under it. Names are split on dots: the backtick quoting of the protocol's field-path
scheme is not supported, so a column whose name contains a dot cannot be given its own key.
Fields inside arrays and maps cannot be named. Partition columns cannot be
encrypted, since their values appear in the log. With column mapping the stored names are
physical names, so renaming a column keeps the configuration valid; delta-rs accepts display
names at creation and stores the physical names.

The log holds no statistics for encrypted columns, so `delta.dataSkippingStatsColumns` must not
name one, and data skipping is not available on them. Under uniform encryption no column
statistics are written at all.

## Registering a KMS

delta-rs never holds master keys. It ships a reference `KmsEncryptionFactory` built on the
[parquet-key-management](https://crates.io/crates/parquet-key-management) crate, the Rust
implementation of the Parquet Key Management Tools used by parquet-java and PyArrow. The crate
generates fresh data keys for every file, asks a key management system (KMS) to wrap them under
the master key IDs named in the table properties, and stores the wrapped keys in the file's own
Parquet key metadata. Readers ask the same KMS to unwrap them. The KMS is the crate's two-method
trait `KmsClient` (or `AsyncKmsClient`), with `wrap_key` and `unwrap_key`; implement it for your
service, give the factory a function that builds a `CryptoFactory` from it, and register the
factory under the table's `kms_id` before using the table:

```rust
use deltalake::operations::write::encryption::parquet_key_management::crypto_factory::CryptoFactory;
use deltalake::operations::write::encryption::{KmsEncryptionFactory, register_encryption_factory};

let factory = KmsEncryptionFactory::new(|| {
    CryptoFactory::new(|info| Ok(Arc::new(MyKms::connect(info)?)))
});
register_encryption_factory("my-kms", Arc::new(factory));
```

The crate is re-exported, so no separate dependency is needed. The client factory receives the
KMS instance ID, which is the table's `kms_id`, and the table's `kms_configuration` as the custom
option `delta.encryption.kms_configuration`. The crate caches clients by instance ID, URL and
access token, so delta-rs builds one `CryptoFactory` per distinct `kms_configuration`: tables
that differ in it never share a client, and a changed `kms_configuration` takes effect on the
next file.

The key metadata is the crate's PKMT1 key material with double wrapping, so files written by
delta-rs can be read by parquet-java and PyArrow through an equivalent KMS client, and the other
way round. Decryption depends only on the KMS and the key metadata in each file, never on the
table's current `footer_key` or `column_keys`, so files written under earlier key settings stay
readable.

The `basic_operations_encryption` example in the `deltalake` crate shows a complete round trip
with an in-memory stand-in for a KMS:

```shell
cargo run --example basic_operations_encryption -p deltalake --features "datafusion encryption"
```

## Changing the configuration

Encryption is set when a table is created and is frozen after that. Every commit is checked:

| Change                                                           | Allowed?                                    |
|------------------------------------------------------------------|---------------------------------------------|
| Set encryption when creating a table                             | Yes                                         |
| Set or change encryption with create-or-replace                  | Yes, every data file is replaced            |
| Change `delta.encryption.kms_configuration`                      | Yes                                         |
| Drop an encrypted column, which drops it from `column_keys`      | Yes, unless it is the last one              |
| Turn encryption on or off for an existing table                  | No                                          |
| Change `kms_id`, `footer_key`, `plaintext_footer` or `column_keys` | No                                       |
| Restore to a version with a different configuration              | No                                          |
| Create a table from existing data files with encryption          | No, the files are plaintext                 |

Configurations are compared as parsed values, so reordering `column_keys` or spelling the
`plaintext_footer` default is not a change.

The changes are refused because delta-rs has no operation yet that rewrites every data file under
a new configuration in one commit. Without it, a later rewrite (optimize, update, delete, merge)
would re-emit old rows under the new configuration, and old plaintext files and statistics
would stay behind. Mixed files are valid Parquet and the RFC allows the changes, so this is
delta-rs behaviour that can be relaxed once that operation exists. To change a table's
encryption today, copy its data into a new table, or recreate it with create-or-replace.

A replace removes every data file in one commit, but the removed files stay in storage until
`vacuum` runs, and the old log entries with their statistics until log cleanup.

Adding columns to a table with `column_keys` leaves the new columns unencrypted, as the RFC
specifies; the write logs a warning naming them. Under uniform encryption new columns are
encrypted with the footer key. Dropping an encrypted column removes it from `column_keys`,
as the RFC requires; dropping the last one is refused, since an empty `column_keys` would
mean uniform encryption.

Passing `delta.encryption.*` properties to a write on an existing table is an error when they
differ from the table's, since a write does not change the configuration of an existing table.
Matching values pass, so a pipeline can resend the same configuration on every run, with
column names as stored or as displayed. A different `kms_configuration` also passes, since
it is not frozen, but is not applied either: change it with the table-property operations.

## What stays plaintext

Encryption covers the Parquet data files only. The Delta log still shows the schema, partition
values, each file's row count and size, and the encryption properties, including `kms_id` and
the master key IDs; each file's key metadata repeats those IDs. Deletion vectors, which record
the positions of deleted rows, are not encrypted. With `plaintext_footer` the footer shows which
columns are encrypted, but the Parquet-level statistics of encrypted columns stay encrypted
either way. Keys are held in process memory while a file is written or read, so the design does
not protect against an attacker who can read the memory of the writing or reading process.

## Protocol

The RFC guards encrypted tables with a `parquetEncryption` table feature so that engines
without encryption support refuse them. While the RFC is a draft the name carries the `-dev`
suffix the RFC process asks of experimental implementations, `parquetEncryption-dev`; once it
is standardised, delta-rs will accept both names. delta-kernel does not accept the feature yet,
so delta-rs does not add it and recognises encrypted tables by their properties instead. Until
the feature can be added, other engines are not protected.

Any `delta.encryption.*` key marks a table as encrypted, valid or not, so a misconfigured
table is refused rather than read as ciphertext or written as plaintext. A stray key without
a footer key, which another engine may leave on a plaintext table, therefore locks it out of
delta-rs; the configuration is then invalid, so no commit can remove the key, and the way out
is create-or-replace. A commit may only introduce the `delta.encryption.*` keys listed above,
which catches typos; a key a later RFC revision adds is tolerated once it is in the table, so
that the feature flag, not the property names, decides whether delta-rs can open a table.
