'''import pandas as pd

FILE = r"C:/Users/aribh/Downloads/2026-01/yellow_tripdata_2026-01.parquet"

df = pd.read_parquet(FILE)

print("\n=== Shape ===")
print(df.shape)

print("\n=== Columns ===")
print(df.columns.tolist())

print("\n=== Data types ===")
print(df.dtypes)

print("\n=== First 5 rows ===")
print(df.head().to_string())

print("\n=== Missing values ===")
print(df.isna().sum())
'''
import os
from dotenv import load_dotenv
import pyarrow.parquet as pq

load_dotenv()

TEST_FILE_PATH = os.getenv("TEST_FILE_PATH")

def inspect_parquet(file: str) -> None:
    parquet_file = pq.ParquetFile(file)

    print("Rows:", parquet_file.metadata.num_rows)
    print("Row groups:", parquet_file.num_row_groups)
    print("Columns:", parquet_file.schema.names)

    for i in range(min(5, parquet_file.num_row_groups)):
        row_group = parquet_file.metadata.row_group(i)
        print(
            f"Row group {i}: "
            f"{row_group.num_rows:,} rows"
        )

if __name__ == "__main__":
    inspect_parquet(TEST_FILE_PATH)