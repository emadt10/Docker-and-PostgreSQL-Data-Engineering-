#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine

# ----------------------------
# 1️⃣ Column data types
# ----------------------------
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

# ----------------------------
# 2️⃣ Columns to parse as dates
# ----------------------------
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

# ----------------------------
# 3️⃣ Function to ingest data in chunks
# ----------------------------
def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100_000,
) -> None:
    """
    Read CSV file in chunks and ingest into PostgreSQL table.
    """

    # Create an iterator for CSV chunks
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        chunksize=chunksize
    )

    # ----------------------------
    # 3a. Process first chunk
    # ----------------------------
    first_chunk = next(df_iter)

    # Create table schema only (no data)
    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace",
        index=False
    )
    print(f"Table '{target_table}' created")

    # Insert first chunk
    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append",
        index=False
    )
    print(f"Inserted first chunk: {len(first_chunk)} rows")

    # ----------------------------
    # 3b. Process remaining chunks
    # ----------------------------
    for df_chunk in df_iter:
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False
        )
        print(f"Inserted chunk: {len(df_chunk)} rows")

    print(f"Done ingesting data into '{target_table}'")

# ----------------------------
# 4️⃣ Main function
# ----------------------------
def main():
    # PostgreSQL connection details
    pg_user = 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_port = '5432'
    pg_db = 'ny_taxi'

    # File details
    year = 2021
    month = 1
    chunksize = 100_000
    target_table = 'yellow_taxi_data'

    # Database engine
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    # Construct file URL
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'

    # Ingest data
    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )

# ----------------------------
# Entry point
# ----------------------------
if __name__ == '__main__':
    main()
