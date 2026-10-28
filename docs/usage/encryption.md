# Encrypting a table

delta-rs can encrypt a table's Parquet data files with
[Parquet Modular Encryption](https://parquet.apache.org/docs/file-format/data-pages/encryption/),
following the Delta protocol RFC for
[Parquet encryption](https://github.com/delta-io/delta/issues/6195).
Encryption is configured with table properties when the table is created.

!!! warning "Experimental and not yet usable"
    Parquet encryption follows a draft RFC that is not yet part of the Delta protocol. The
    properties, the feature name and the behaviour described here may change or be removed.
    This release only defines and validates the properties: writing and reading encrypted
    tables is not supported yet, and every build refuses encrypted tables until that support
    lands.

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

`footer_key` is always required: Parquet encrypts the footer with it, or signs the footer with
it under `plaintext_footer`. There is no footer-less mode. An encrypted footer hides which
columns are encrypted and the Parquet-level statistics. This is stricter than the RFC, which
lets a table keep `kms_id` and `kms_configuration` after its footer key was unset, meaning
encryption was turned off; delta-rs refuses such a table rather than treat it as plaintext.

Column names in `column_keys` may be dot-separated to name a struct field, which encrypts every
field under it, and use the protocol's backtick quoting for a name that contains a dot. Names
are matched case-insensitively, as partition and clustering columns are. Fields inside arrays
and maps cannot be named. Partition columns cannot be
encrypted, since their values appear in the log. With column mapping the stored names are
physical names, so renaming a column keeps the configuration valid; delta-rs accepts display
names at creation and stores the physical names.

The log holds no statistics for encrypted columns, so `delta.dataSkippingStatsColumns` must not
include encrypted columns, and data skipping is not available on them. Under uniform encryption no column
statistics are written at all.

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
specifies. Under uniform encryption new columns are encrypted with the footer key. Dropping an
encrypted column removes it from `column_keys`, as the RFC requires; dropping the last one is
refused, since an empty `column_keys` would mean uniform encryption.

Passing `delta.encryption.*` properties to a write on an existing table is an error when they
differ from the table's, since a write does not change the configuration of an existing table.
Matching values pass, so a pipeline can resend the same configuration on every run, with
column names as stored or as displayed. A different `kms_configuration` also passes, since
it is not frozen, but is not applied either: change it with the table-property operations.

## Protocol

The RFC guards encrypted tables with a `parquetEncryption` table feature so that engines
without encryption support refuse them. While the RFC is a draft the name carries the `-dev`
suffix the RFC process asks of experimental implementations, `parquetEncryption-dev`; once it
is standardised, both names will be accepted. Any `delta.encryption.*` property at table
creation adds the feature to the protocol as a reader and writer feature (reader version 3,
writer version 7), and delta-kernel parses the properties and applies the RFC's CREATE TABLE
rules to them. Builds of delta-rs without encryption support refuse the feature like any other
unsupported one, and kernel refuses to write to such a table on its own.

A table with the feature is encrypted whether or not its properties are valid, so a
misconfigured table is refused rather than read as ciphertext or written as plaintext. A stray
key without a footer key, which another engine may leave on a plaintext table, therefore locks
it out of delta-rs; the configuration is then invalid, so no commit can remove the key, and
the way out is create-or-replace. A commit may only introduce the `delta.encryption.*` keys listed above,
which catches typos; a key a later RFC revision adds is tolerated once it is in the table, so
that the feature flag, not the property names, decides whether delta-rs can open a table.
