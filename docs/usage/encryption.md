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

`footer_key` is always required: Parquet encrypts the footer with it, or signs the footer with
it under `plaintext_footer`. There is no footer-less mode. An encrypted footer hides the schema,
row counts and which columns are encrypted.

Column names in `column_keys` may be dot-separated to name a struct field, which encrypts every
field under it. Fields inside arrays and maps cannot be named. Partition columns cannot be
encrypted, since their values appear in the log. With column mapping the stored names are
physical names, so renaming a column keeps the configuration valid; delta-rs accepts display
names at creation and stores the physical names.

The log holds no statistics for encrypted columns, so `delta.dataSkippingStatsColumns` must not
name one, and data skipping is not available on them. Under uniform encryption no column
statistics are written at all.

## Changing the configuration

Encryption is set when a table is created and is frozen after that. Every commit is checked:

| Change                                                           | Allowed?                                    |
|------------------------------------------------------------------|---------------------------------------------|
| Set encryption when creating a table                             | Yes                                         |
| Set or change encryption with create-or-replace                  | Yes, every data file is replaced            |
| Change `delta.encryption.kms_configuration`                      | Yes                                         |
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
encrypted with the footer key.

Passing `delta.encryption.*` properties to a write on an existing table is an error, since a
write does not change the configuration of an existing table.

## Protocol

The RFC guards encrypted tables with a `parquetEncryption` table feature so that engines
without encryption support refuse them. delta-kernel does not accept that feature yet, so
delta-rs does not add it and recognises encrypted tables by their properties instead. Until
the feature can be added, other engines are not protected.
