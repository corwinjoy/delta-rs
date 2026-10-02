# simple_encrypted_table

A metadata-only Delta table (no data files) showing the protocol and table
properties of an encrypted table as proposed in the Delta protocol RFC
<https://github.com/delta-io/delta/issues/6195>.

- `ssn` is encrypted with master key `pii-key` and `salary` with `finance-key`;
  `id` and `name` are not encrypted. The footer is encrypted with `footer-key`.
- `kms_id` names the KMS client readers and writers must register;
  `kms_configuration` is an opaque string passed to it.

delta-kernel does not support the `parquetEncryption` feature yet, so
`open_table` rejects this table; tests read its log directly.
