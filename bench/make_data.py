from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

compressions = ["SNAPPY", "GZIP", "BROTLI", "ZSTD", "NONE"]


def create_table(n_rows=1_000_000):
    data = {}

    for dtype in ["uint8", "uint16", "uint32"]:
        data[dtype] = pa.array(np.random.randint(0, np.iinfo(dtype).max, size=n_rows))

    data["bool"] = pa.array(np.random.randint(0, 2, size=n_rows), type=pa.bool_())

    # Todo column with string data?
    # https://stackoverflow.com/a/2257449

    return pa.table(data)


def write_table(table):
    # Create data directory
    Path("data").mkdir(exist_ok=True)

    data_len = len(table)
    for n_partitions in [1, 5, 20]:
        for compression in compressions:
            row_group_size = data_len / n_partitions
            compression_text = str(compression).lower()
            fname = f"data/{n_partitions}-partition-{compression_text}.parquet"
            pq.write_table(
                table, fname, row_group_size=row_group_size, compression=compression
            )


def main():
    table = create_table()
    write_table(table)


if __name__ == "__main__":
    main()
