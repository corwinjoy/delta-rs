# Encrypting a table

delta-rs can encrypt the Parquet data files of a table with
[Parquet Modular Encryption](https://parquet.apache.org/docs/file-format/data-pages/encryption/),
following the Delta protocol RFC for
[Parquet encryption](https://github.com/delta-io/delta/issues/6195).
Encryption is configured with table properties and is turned on when the table is created.

Encryption support is an opt-in cargo feature (`encryption`) of the Rust crates. Builds without
it refuse to read or write encrypted tables, so they never return ciphertext or add plaintext
files to an encrypted table.

## Table properties

| Property                             | Meaning                                                                                                            |
|--------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `delta.encryption.kms_id`            | Which registered key management system (KMS) client to use. Required.                                              |
| `delta.encryption.kms_configuration` | Opaque, KMS-specific configuration, for example a JSON document. Optional.                                         |
| `delta.encryption.footer_key`        | Master key ID for the footer. Setting it turns encryption on.                                                      |
| `delta.encryption.plaintext_footer`  | `true` leaves the footer readable but signed with the footer key. Defaults to `false`.                             |
| `delta.encryption.column_keys`       | Columns to encrypt with their master key IDs, as `keyId:col1,col2;keyId2:col3`. Empty means every column is encrypted with the footer key. |

The properties are stored in plaintext in the Delta log, so they must only hold key IDs and
KMS settings, never keys or credentials. Key material for each data file is stored in the
file's own Parquet key metadata.

`footer_key` is always required: Parquet has no mode that encrypts some columns and leaves the
footer alone. By default the footer is encrypted, which hides the schema, row counts and which
columns are encrypted. With `plaintext_footer` it stays readable but is signed so tampering
is detected.

Column names in `column_keys` may be dot-separated to name a field of a struct, which
encrypts every field under it. Fields inside arrays and maps cannot be named. Partition columns
cannot be encrypted, since their values appear in the Delta log. With column mapping, the
stored names are physical names, so renaming a column does not invalidate the configuration;
delta-rs accepts display names when the table is created and stores the physical names.

The Delta log holds no statistics for encrypted columns, so `delta.dataSkippingStatsColumns`
must not name one, and data skipping is not available on encrypted columns. Under uniform
encryption (no `column_keys`) no column statistics are written at all.

## Changing the configuration

Encryption is set when a table is created and is fixed from then on. Every commit to an
existing table is checked, and the following are refused:

| Change                                                      | Allowed?                                    |
|-------------------------------------------------------------|---------------------------------------------|
| Set encryption when creating a table                        | Yes                                         |
| Set or change encryption with create-or-replace             | Yes, every data file is replaced            |
| Change `delta.encryption.kms_configuration`                 | Yes, at any time                            |
| Turn encryption on for an existing table                    | No                                          |
| Turn encryption off                                         | No                                          |
| Change `kms_id`, `footer_key`, `plaintext_footer` or `column_keys` | No                                   |
| Restore to a version with a different configuration         | No                                          |
| Create a table from existing data files with encryption     | No, the files are not encrypted             |

Configurations are compared as parsed values, so reordering `column_keys` or spelling the
`plaintext_footer` default explicitly is not a change.

These changes are refused because delta-rs does not yet have an operation that rewrites every
data file under a new configuration in one commit. Without that, a later rewrite (optimize,
update, delete or merge) would silently re-emit old rows under the new configuration, and old
plaintext files and statistics would stay behind. Mixed files are valid for Parquet and the
RFC allows the changes, so this is delta-rs behaviour that can be relaxed once that operation
exists. To change the encryption of a table today, copy its data into a new table with the
configuration you want, or recreate it with create-or-replace.

Replacing a table removes every data file in the same commit, but the removed files stay in
storage until `vacuum` runs, and the old log entries with their column statistics until log
cleanup. Run `vacuum` after replacing a plaintext table with an encrypted one.

Adding columns to a table with `column_keys` leaves the new columns unencrypted, as the RFC
specifies; the write logs a warning naming them. Under uniform encryption new columns are
encrypted with the footer key.

Passing `delta.encryption.*` properties in the configuration of a write to a table that
already exists is an error, since a write does not change the configuration of an existing
table.

## Protocol

The RFC protects encrypted tables with a `parquetEncryption` table feature so that engines
without encryption support refuse them. delta-kernel does not accept that feature yet, so
delta-rs does not add it when creating a table. delta-rs recognises encrypted tables by their
`delta.encryption.*` properties and refuses to read or write them in builds without
encryption support. Until the feature can be added, other engines are not protected.
