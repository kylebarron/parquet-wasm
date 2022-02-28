import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq

compressions = ["SNAPPY", "GZIP", "BROTLI", "LZ4", "ZSTD", 'NONE']


def create_data():
    data = {
        "str": pa.array(["a", "b", "c", "d"], type=pa.string()),
        "uint8": pa.array([1, 2, 3, 4], type=pa.uint8()),
        "int32": pa.array([0, -2147483638, 2147483637, 1], type=pa.int32()),
        "bool": pa.array([True, True, False, False], type=pa.bool_()),
    }
    return pa.table(data)


def write_data(table):
    feather.write_feather(table, "data.arrow", compression="uncompressed")

    data_len = len(table)

    for n_partitions in [1, 2]:
        for compression in compressions:
            row_group_size = data_len / n_partitions
            compression_text = str(compression).lower()
            fname = f"{n_partitions}-partition-{compression_text}.parquet"
            pq.write_table(
                table, fname, row_group_size=row_group_size, compression=compression
            )


def main():
    table = create_data()
    write_data(table)


def debug():
    """Create debug data"""
    works = pa.table({
        "uint8": pa.array([1, 2, 3, 4], type=pa.uint8()),
    })
    pq.write_table(works, 'works.parquet', compression='NONE')

    not_work = pa.table({
        "int8": pa.array([0, 0, 0, 0], type=pa.int8()),
    })
    pq.write_table(not_work, 'not_work.parquet', compression='NONE')


if __name__ == "__main__":
    main()
    debug()
