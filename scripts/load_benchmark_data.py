#!/usr/bin/env python3
"""
Benchmark Data Loader

Loads NIFTY index data from delta table to clean parquet file.
"""

from config.settings import R2


def load_benchmark_data(connection, source_path, target_path):
    """
    Load benchmark data from delta table to parquet.

    Args:
        connection: DuckDB connection
        source_path: Source delta table path
        target_path: Target parquet file path
    """
    connection.execute(f"""
        COPY (
            SELECT *
            FROM delta_scan('{source_path}')
        )
        TO '{target_path}'
        (FORMAT PARQUET)
    """)


def main():
    try:
        r2 = R2()
        conn = r2.setup_connection()

        source = 'r2://financial-data-store/bronze/nseindex/daily_price_nifty_indices'
        target = r2.get_full_path('clean', 'mf_benchmark_nifty')

        load_benchmark_data(connection=conn, source_path=source, target_path=target)

        print(f"✅ Successfully loaded benchmark data to {target}")
        print(conn.read_parquet(target).limit(5))

    except Exception as e:
        print(f"❌ Error during processing: {e}")
        return False
    return True


if __name__ == "__main__":
    main()
