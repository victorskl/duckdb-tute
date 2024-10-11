from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa


def print_line(f):
    def wrapped(*args, **kwargs):
        _ = f(*args, **kwargs)
        print("-" * 64)

    return wrapped


@print_line
def basic():
    print(duckdb.sql("SELECT 42").show())

    r1 = duckdb.sql("SELECT 42 AS i")
    print(r1)
    print(duckdb.sql("SELECT i * 2 AS k FROM r1").show())


@print_line
def data_input():
    r2a = duckdb.read_csv("../sample-data/customers-100.csv")
    r2b = duckdb.sql("SELECT * FROM '../sample-data/customers-100.csv' LIMIT 10")
    print(r2b)
    print(type(r2a), type(r2b))


@print_line
def dataframe_pandas():
    pandas_df = pd.DataFrame({"a": [42]})
    r3 = duckdb.sql("SELECT * FROM pandas_df")
    print(r3)


@print_line
def dataframe_polars():
    polars_df = pl.DataFrame({"a": [42]})
    r4 = duckdb.sql("SELECT * FROM polars_df")
    print(r4)

    # BUT. Polars has SQL syntax support too.! :P
    # https://docs.pola.rs/api/python/stable/reference/sql/index.html
    df = polars_df.sql("""
      SELECT * FROM self
    """)
    print(df)


@print_line
def dataframe_pyarrow():
    arrow_table = pa.Table.from_pydict({"a": [42]})
    r5 = duckdb.sql("SELECT * FROM arrow_table")
    print(r5)


def result_conversion():
    print(duckdb.sql("SELECT 62").fetchall())  # Python objects
    print(duckdb.sql("SELECT 62").df())  # Pandas DataFrame
    print(duckdb.sql("SELECT 62").pl())  # Polars DataFrame
    print(duckdb.sql("SELECT 62").arrow())  # Arrow Table
    print(duckdb.sql("SELECT 62").fetchnumpy())  # NumPy Arrays


def write_data():
    Path.mkdir(Path("./tmp"), exist_ok=True)
    duckdb.sql("SELECT 42").write_parquet("tmp/out.parquet")  # Write to a Parquet file
    duckdb.sql("SELECT 42").write_csv("tmp/out.csv")  # Write to a CSV file
    duckdb.sql("COPY (SELECT 42) TO 'tmp/out.parquet'")  # Copy to a Parquet file


if __name__ == '__main__':
    basic()
    data_input()
    dataframe_pandas()
    dataframe_polars()
    dataframe_pyarrow()
    result_conversion()
    write_data()
