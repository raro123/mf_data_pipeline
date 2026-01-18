#!/usr/bin/env python3
"""
Daily NAV Data Cleaner

Joins raw NAV data with scheme metadata to create enriched daily NAV data.
Filters for growth plans only.
"""

import argparse
from config.settings import R2
from utils.nav_helpers import save_to_parquet


def parse_args():
    parser = argparse.ArgumentParser(description='Clean daily NAV data')
    parser.add_argument('--date', type=str, help='Specific date to process (YYYYMMDD format)')
    return parser.parse_args()


def create_daily_nav(raw_data_path, metadata_path, connection):
    """
    Create enriched daily NAV data by joining raw NAV with scheme metadata.

    Args:
        raw_data_path: Path pattern for raw NAV parquet files
        metadata_path: Path to scheme metadata parquet file
        connection: DuckDB connection

    Returns:
        DuckDB relation with enriched NAV data
    """
    metadata = connection.read_parquet(metadata_path)
    all_raw = connection.read_parquet(raw_data_path)
    # Join with growth plan metadata and select relevant columns
    nav_data = (all_raw
                .join(metadata
                      .filter('is_growth_plan = TRUE')
                      .select('''scheme_code,
                                 amc_name,
                                 scheme_name,
                                 scheme_type,
                                 scheme_category,
                                 scheme_nav_name,
                                 scheme_category_level1,
                                 scheme_category_level2,
                                 is_direct,
                                 is_growth_plan'''),
                      "scheme_code", how="inner")
                .filter("nav IS NOT NULL")
                .distinct())
    return nav_data


def main():
    args = parse_args()

    try:
        r2 = R2()
        conn = r2.setup_connection()

        if args.date:
            raw_data_path = r2.get_full_path('raw', f'nav_daily_{args.date}')
        else:
            raw_data_path = r2.get_full_path('raw', "*")

        metadata_path = r2.get_full_path('clean', 'scheme_metadata')

        clean_df = create_daily_nav(
            raw_data_path=raw_data_path, metadata_path=metadata_path, connection=conn)
        path = r2.get_full_path('clean', 'nav_daily_growth_plan')
        save_to_parquet(conn, 'nav_daily_growth_plan', clean_df, path)
        print(f"Successfully created daily NAV Parquet file at {path}")
        print(conn.read_parquet(path).limit(5))
    except Exception as e:
        print(f"Error during processing: {e}")
        return False
    return True


if __name__ == "__main__":
    main()
