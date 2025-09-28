from config.settings import R2
from config.settings import Paths


r2 = R2()
conn = r2.setup_connection()
raw_data_path = r2.get_full_path('raw', "*")
metdata_path = r2.get_full_path('clean', 'scheme_metadata')



def create_daily_nav(raw_data_path, metadata_path, connection):
    # Read all Parquet files from the specified path into a single DuckDB table
    metadata = connection.read_parquet(metadata_path)
    all_raw = connection.read_parquet(raw_data_path)
    # Perform a join with the metadata table to enrich the data
    nav_data = all_raw.join(metadata.filter('is_growth_plan = TRUE').select('''scheme_code,
        amc_name,
        scheme_name,
        scheme_type,
        scheme_category,
        scheme_nav_name,
        scheme_category_level1,
        scheme_category_level2,
        is_direct,
        is_growth_plan'''), "scheme_code", how="inner").filter("nav IS NOT NULL").distinct()
    return nav_data


def load_nav(table_name, table_df, path, connection):
    # Register your DataFrame with DuckDB
    connection.register(table_name, table_df)
    connection.execute(f"""
    COPY {table_name} TO '{path}' 
    (FORMAT PARQUET)
    """)


def main():
    try:
        clean_df = create_daily_nav(
            raw_data_path=raw_data_path, metadata_path=metdata_path, connection=conn)
        path = r2.get_full_path('clean', 'nav_daily_growth_plan')
        load_nav(table_name='nav_daily_growth_plan',
                 table_df=clean_df, path=path, connection=conn)
        print(f"✅ Successfully created daily NAV Parquet file at {path}")
        print(conn.read_parquet(path).limit(5))
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        return False
    return True


if __name__ == "__main__":
    main()
