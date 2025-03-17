from common import *

import pandas as pd
import pyarrow.parquet as pq
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import math


# Use the deltalake table reader
def read_delta_table(dl_path):
    dt = DeltaTable(dl_path)
    df_files = dt.file_uris()
    df_dl = dt.to_pandas()
    return (df_files, df_dl)

# Use pyarrow to directly read the underlying parquet files and merge
def read_delta_files(df_files):
    tbls = []
    for file in df_files:
        tbl = pq.read_table(file)
        tbls.append(tbl.to_pandas())
    all_tbls = pd.concat(tbls, ignore_index=True)
    return all_tbls

def run_bm_scenarios():
    # One gigabyte of float32 data translates to the following number of rows and cols
    # 1024^3 (1 GiB) / 4 (bytes per float 32) / 128 cols
    # 2_097_152
    df_bm = None
    nrows = math.ceil(2_097_152 / 10) # Up to 10 reps
    for nappend in range(0, 10):
        for ncols in range(20, 140, 20):
            clear_folder(dl_folder)
            df = gen_df(nrows, ncols)
            write_deltalake(dl_path, df)
            if nappend > 0:
                for i in range(nappend):
                    df = gen_df(nrows, ncols)
                    write_deltalake(dl_path, df, mode="append")

            tbl_tm = timed(read_delta_table, dl_path)
            (df_files, df_dl) = tbl_tm[0]
            tm_delta = tbl_tm[1]
            # print("delta table:")
            # print(df_dl)
            pq_tm = timed(read_delta_files, df_files)
            tm_pq = pq_tm[1]
            # print("pq table:")
            # print(pq_tm[0])
            df_tmp = pd.DataFrame({"nrows": [nrows], "ncols": [ncols], "nappend": [nappend],
                                   "time_delta": [tm_delta], "time_pq": [tm_pq]})
            print(df_tmp)
            if df_bm is None:
                df_bm = df_tmp
            else:
                df_bm = pd.concat([df_bm, df_tmp], ignore_index=True)

    print("\n----------------------------------------------\n")
    print("Collected benchmark results:")
    print(df_bm)
    df_bm.to_csv(dl_folder + "/../delta_benchmark.csv")

# Run deltalake benchmarks
run_bm_scenarios()
