# simple_encrypted_table

Metadata-only Delta table (no data files) showing the table properties and
protocol for Parquet Modular Encryption as proposed in the Delta protocol RFC
<https://github.com/delta-io/delta/issues/6195>.

- `ssn` is encrypted with master key `pii-key`, `salary` with `finance-key`.
- `id` and `name` are not in `column_keys`, so they are left unencrypted; the
  footer is encrypted with `footer-key`.
- `kms_id` names the KMS client a reader/writer must register before using the
  table; `kms_configuration` is an opaque string handed to that client.

The `parquetEncryption` table feature is not yet known to delta-kernel, so
this table cannot be opened with `open_table` yet; tests read the log directly.
