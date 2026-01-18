"""
NAV Data Processing Utilities

Shared utilities for NAV data transformation and storage used across multiple scripts.
"""

import pandas as pd

# Column mapping from raw AMFI format to standardized names
NAV_COLUMN_MAPPING = {
    'Scheme Code': 'scheme_code',
    'ISIN Div Payout/ISIN Growth': 'isin_growth',
    'ISIN Div Reinvestment': 'isin_dividend',
    'Net Asset Value': 'nav',
    'Date': 'date'
}

NAV_COLUMNS = list(NAV_COLUMN_MAPPING.keys())


def clean_nav_dataframe(df: pd.DataFrame, columns: list = None) -> pd.DataFrame:
    """
    Standardize NAV DataFrame columns and types.

    Args:
        df: Raw NAV DataFrame from AMFI
        columns: List of columns to select (defaults to NAV_COLUMNS)

    Returns:
        Cleaned DataFrame with standardized column names and types
    """
    columns = columns or NAV_COLUMNS
    return (df[columns]
            .rename(columns=NAV_COLUMN_MAPPING)
            .query('scheme_code.notnull() & nav.notnull() & date.notnull()')
            .assign(
                scheme_code=lambda x: x['scheme_code'].astype(str),
                date=lambda x: pd.to_datetime(x['date'], format='%d-%b-%Y', errors='coerce'),
                nav=lambda x: pd.to_numeric(x['nav'], errors='coerce')
            ))


def save_to_parquet(connection, table_name: str, df, path: str):
    """
    Save DataFrame to Parquet via DuckDB.

    Args:
        connection: DuckDB connection object
        table_name: Name to register the table as
        df: DataFrame or DuckDB relation to save
        path: Output path for Parquet file
    """
    connection.register(table_name, df)
    connection.execute(f"COPY {table_name} TO '{path}' (FORMAT PARQUET)")
