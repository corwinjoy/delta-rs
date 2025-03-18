from common import *
import pyarrow as pa
import pyarrow.parquet.encryption as pe
import pyarrow.dataset as ds
from deltalake.writer import write_deltalake
from deltalake.writer.properties import PyEncryptionProperties
from deltalake import DeltaTable, WriterProperties
import math
import base64

from pyarrow._parquet import FileEncryptionProperties
from tests.conftest import writer_properties

FOOTER_KEY = b"1234567890123450"
FOOTER_KEY_NAME = "footer_key"
COL_KEY = b"1234567890123450"
COL_KEY_NAME = "col_key"

def read_delta_table(dl_path, fragment_scan_options):
    dt = DeltaTable(dl_path)
    df_files = dt.file_uris()
    df_dl = dt.to_pandas(fragment_scan_options=fragment_scan_options)
    return (df_files, df_dl)

class InMemoryKmsClient(pe.KmsClient):
    """This is a mock class implementation of KmsClient, built for testing
    only.
    """

    def __init__(self, config):
        """Create an InMemoryKmsClient instance."""
        pe.KmsClient.__init__(self)
        self.master_keys_map = config.custom_kms_conf

    def wrap_key(self, key_bytes, master_key_identifier):
        result = self.master_keys_map[master_key_identifier].encode(
            'utf-8')
        print("wrap_key", result, master_key_identifier)
        return result

    def unwrap_key(self, wrapped_key, master_key_identifier):
        result = self.master_keys_map[master_key_identifier].encode(
            'utf-8')
        print("unwrap_key", result, master_key_identifier)
        return result

    def wrap_key_orig(self, key_bytes, master_key_identifier):
        """Not a secure cipher - the wrapped key
        is just the master key concatenated with key bytes"""
        master_key_bytes = self.master_keys_map[master_key_identifier].encode(
            'utf-8')
        wrapped_key = b"".join([master_key_bytes, key_bytes])
        result = base64.b64encode(wrapped_key)
        return result

    def unwrap_key_orig(self, wrapped_key, master_key_identifier):
        """Not a secure cipher - just extract the key from
        the wrapped key"""
        expected_master_key = self.master_keys_map[master_key_identifier]
        decoded_wrapped_key = base64.b64decode(wrapped_key)
        master_key_bytes = decoded_wrapped_key[:16]
        decrypted_key = decoded_wrapped_key[16:]
        if (expected_master_key == master_key_bytes.decode('utf-8')):
            return decrypted_key
        raise ValueError("Incorrect master key used",
                         master_key_bytes, decrypted_key)

def create_encryption_config(df):
    return pe.EncryptionConfiguration(
        footer_key=FOOTER_KEY_NAME,
        column_keys={
            COL_KEY_NAME: df.columns.tolist(),
        })


def create_decryption_config():
    return pe.DecryptionConfiguration(cache_lifetime=300)


def create_kms_connection_config():
    custom_kms_conf = {
        FOOTER_KEY_NAME: FOOTER_KEY.decode("UTF-8"),
        COL_KEY_NAME: COL_KEY.decode("UTF-8"),
    }
    print(custom_kms_conf)
    return pe.KmsConnectionConfig(
        custom_kms_conf=custom_kms_conf,
    )

def kms_factory(kms_connection_configuration):
    return InMemoryKmsClient(kms_connection_configuration)


def run_delta_ecrypt():
    # Test encryption with delta lake
    # The interface looks like it should support encryption, but
    # does not support encryption in practice.
    nrows = math.ceil(2_097_152 / 10)  # Up to 10 reps
    nappend = 1
    ncols = 20
    clear_folder(dl_folder)
    df = gen_df(nrows, ncols)

    column_keys = dict()
    keys = df.columns.tolist()
    for key in keys:
        column_keys[key] = COL_KEY
    file_encryption_properties = PyEncryptionProperties(footer_key = FOOTER_KEY, column_keys = column_keys)
    print(file_encryption_properties)

    writer_properties = WriterProperties(file_encryption_properties=file_encryption_properties)

    write_deltalake(dl_path, df, writer_properties=writer_properties)
    # write_deltalake(dl_path, df, engine="pyarrow")
    if nappend > 0:
        for i in range(nappend):
            df = gen_df(nrows, ncols)
            write_deltalake(dl_path, df, mode="append", writer_properties=writer_properties)

def run_delta_decrypt():
    crypto_factory = pe.CryptoFactory(kms_factory)
    kms_connection_config = create_kms_connection_config()
    decryption_config = create_decryption_config()
    parquet_decryption_cfg = ds.ParquetDecryptionConfig(
        crypto_factory, kms_connection_config, decryption_config
    )
    fragment_scan_options = ds.ParquetFragmentScanOptions(
        pre_buffer=True,
        decryption_config=parquet_decryption_cfg
    )
    tbl_tm = timed(read_delta_table, dl_path, fragment_scan_options)
    (df_files, df_dl) = tbl_tm[0]
    tm_delta = tbl_tm[1]
    print("delta table:")
    print(df_dl)


def run_pq_encrypt():
    # Test encryption with pyarrow
    nrows = math.ceil(2_097_152 / 10)  # Up to 10 reps
    ncols = 128 # 100 MB of data
    # clear_folder(pq_folder)
    df = gen_df(nrows, ncols)
    table = pa.Table.from_pandas(df)

    encryption_config = create_encryption_config(df)
    decryption_config = create_decryption_config()
    kms_connection_config = create_kms_connection_config()

    crypto_factory = pe.CryptoFactory(kms_factory)
    parquet_encryption_cfg = ds.ParquetEncryptionConfig(
        crypto_factory, kms_connection_config, encryption_config
    )
    parquet_decryption_cfg = ds.ParquetDecryptionConfig(
        crypto_factory, kms_connection_config, decryption_config
    )

    def read_encrypted_data(parquet_decryption_cfg, df):
        # set decryption config for parquet fragment scan options
        pq_scan_opts = ds.ParquetFragmentScanOptions(
            decryption_config=parquet_decryption_cfg
        )
        pformat = ds.ParquetFileFormat(default_fragment_scan_options=pq_scan_opts)
        dataset = ds.dataset(pq_folder, format=pformat)
        tbl_read = dataset.to_table()
        df2 = tbl_read.to_pandas()
        # pd.testing.assert_frame_equal(df, df2, rtol=1e-2, atol=1e-2)
        return df2

    def read_unencrypted_data(df):
        # set decryption config for parquet fragment scan options
        dataset = ds.dataset(pq_folder, format="parquet")
        tbl_read = dataset.to_table()
        df2 = tbl_read.to_pandas()
        # pd.testing.assert_frame_equal(df, df2, rtol=1e-2, atol=1e-2)
        return df2

    # create write_options with dataset encryption config
    pformat = ds.ParquetFileFormat()
    write_options = pformat.make_write_options(encryption_config=parquet_encryption_cfg)

    if False:
        ds.write_dataset(
            data=table,
            base_dir=pq_folder,
            format=pformat,
            file_options=write_options,
            use_threads=False # Maintain original row order
        )
    decrypt_res = timed(read_encrypted_data, parquet_decryption_cfg, df)
    decrypt_tm = decrypt_res[1]
    print(f"Encrypted read time: {decrypt_tm}")


# Run deltalake benchmarks
# run_bm_scenarios()

# Benchmark encryption of parquet
run_pq_encrypt()

# Try deltalake encryption
# run_delta_ecrypt()
# run_delta_decrypt()