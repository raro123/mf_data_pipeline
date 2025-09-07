# Historical NAV Data

This directory contains the cleaned and processed historical mutual fund NAV data from AMFI (Association of Mutual Funds in India).

## Files

- **batch_01.parquet** - 2,054,016 records (25MB)
- **batch_02.parquet** - 3,044,559 records (43MB) 
- **batch_03.parquet** - 4,979,814 records (76MB)
- **batch_04.parquet** - 4,737,603 records (71MB)
- **batch_05.parquet** - 5,539,481 records (31MB)
- **batch_06.parquet** - 5,571,792 records (31MB)

**Total: 25,927,265 records (276MB)**

## Data Schema

Each batch file contains the following columns:
- `scheme_code` - Unique identifier for the mutual fund scheme
- `scheme_name` - Full name of the mutual fund scheme
- `isin_growth` - ISIN code for growth option
- `isin_dividend` - ISIN code for dividend option  
- `nav` - Net Asset Value (primary metric)
- `repurchase_price` - Repurchase price (mostly null in recent data)
- `sale_price` - Sale price (mostly null in recent data)
- `date` - Date of the NAV record

## Date Range
- **Start:** 2006-04-01
- **End:** 2025-08-22
- **Span:** ~19 years of historical data

## Usage

Load individual batches or combine multiple batches as needed:

```python
import pandas as pd

# Load a single batch
df = pd.read_parquet('batch_01.parquet')

# Load all batches
import glob
batch_files = glob.glob('batch_*.parquet')
df_all = pd.concat([pd.read_parquet(f) for f in batch_files], ignore_index=True)
```

## Processing Details

- **Source:** AMFI NAV historical reports
- **Processed:** August 2024
- **Method:** Parallel processing with data validation
- **Format:** Parquet with Snappy compression
- **Quality:** Cleaned data with headers and invalid records removed