#!/usr/bin/env python3
"""
Historical NAV Data Cleaner with DuckDB

Cleans raw historical NAV CSV files and creates a single merged Parquet file using DuckDB.
This approach is memory-efficient and avoids the complexity of batch processing.
"""

import pandas as pd
from pathlib import Path
from config.settings import R2
from config.settings import Paths

col_select = ['Scheme Code', 'ISIN Div Payout/ISIN Growth', 'ISIN Div Reinvestment', 'Net Asset Value', 'Date']

def transform_historical_nav(raw_data_path,col_select):

    clean_df = (pd.concat([pd.read_csv(f)[col_select] for f in Path(raw_data_path).glob('*.csv')], ignore_index=True)
              .rename(columns={
                  'Scheme Code': 'scheme_code',
                  'ISIN Div Payout/ISIN Growth': 'isin_growth',
                  'ISIN Div Reinvestment': 'isin_dividend',
                  'Net Asset Value': 'nav',
                  'Date': 'date'
              })
              .query('scheme_code.notnull() & nav.notnull() & date.notnull()')
              .assign(scheme_code=lambda x: x['scheme_code'].astype(str),
                      date=lambda x: pd.to_datetime(
                          x['date'], format='%d-%b-%Y', errors='coerce'),
                      nav=lambda x: pd.to_numeric(x['nav'], errors='coerce'),
                      )
              )
    return clean_df

def load_historical_nav(table_name,table_df,path, connection):
    # Register your DataFrame with DuckDB
    connection.register(table_name, table_df)
    connection.execute(f"""
    COPY {table_name} TO '{path}' 
    (FORMAT PARQUET)
    """)
    
def main():
    try:
        r2 = R2()
        conn = r2.setup_connection()
        path = r2.get_full_path('raw','nav_historical')
        clean_df = transform_historical_nav(raw_data_path=Paths.RAW_NAV_CSV,col_select=col_select)
        load_historical_nav(table_name='nav_historical_raw',table_df=clean_df,path=path, connection=conn)
        print(f"✅ Successfully created merged historical NAV Parquet file at {path}")
        print(conn.read_parquet(path).limit(5))
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        return False
    return True


if __name__ == "__main__":
    main()


