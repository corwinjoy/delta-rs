# Python
import pandas as pd
import numpy as np
import shutil
import os
import time

workdir = "."
dl_folder = workdir + "/tmp"
pq_folder = workdir + "/tmp_pq"
dl_path = dl_folder + "/deltars_table"

def clear_folder(folder_path):
    """Clears the contents of a folder without deleting the folder itself."""

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')


def gen_df(nrows, ncols):
    # Create a DataFrame with random data
    letter = 'x'
    cols = [f"{letter}{i}" for i in range(ncols)]
    df = pd.DataFrame(np.random.rand(nrows, ncols), columns=cols, dtype=pd.Float32Dtype())
    return df

def timed(func, *args, **kwargs):
    start = time.time()
    out = func(*args, **kwargs)
    return out, time.time() - start