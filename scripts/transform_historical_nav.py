#!/usr/bin/env python3
"""
Historical NAV Data Cleaner with DuckDB

Cleans raw historical NAV CSV files and creates a single merged Parquet file using DuckDB.
This approach is memory-efficient and avoids the complexity of batch processing.
"""

import pandas as pd
from pathlib import Path
from config.settings import R2, Paths
from utils.nav_helpers import NAV_COLUMNS, clean_nav_dataframe, save_to_parquet


def transform_historical_nav(raw_data_path: str) -> pd.DataFrame:
    """
    Transform all historical NAV CSV files into a single clean DataFrame.

    Args:
        raw_data_path: Path to directory containing raw CSV files

    Returns:
        Cleaned DataFrame with standardized columns
    """
    all_dfs = [pd.read_csv(f)[NAV_COLUMNS] for f in Path(raw_data_path).glob('*.csv')]
    combined_df = pd.concat(all_dfs, ignore_index=True)
    return clean_nav_dataframe(combined_df)


def main():
    try:
        r2 = R2()
        conn = r2.setup_connection()
        path = r2.get_full_path('raw', 'nav_historical')
        clean_df = transform_historical_nav(raw_data_path=Paths.RAW_NAV_CSV)
        save_to_parquet(conn, 'nav_historical_raw', clean_df, path)
        print(f"Successfully created merged historical NAV Parquet file at {path}")
        print(conn.read_parquet(path).limit(5))
    except Exception as e:
        print(f"Error during processing: {e}")
        return False
    return True


if __name__ == "__main__":
    main()
