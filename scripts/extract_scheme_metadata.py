#!/usr/bin/env python3
"""
Scheme Metadata Extractor

Fetches scheme metadata from AMFI portal and saves as Parquet to R2.
"""

import requests
import pandas as pd
from datetime import datetime
from io import StringIO

from config.settings import API, R2
from utils.logging_setup import get_extract_metadata_logger, log_script_start, log_script_end
from utils.nav_helpers import save_to_parquet

logger = get_extract_metadata_logger(__name__)


def fetch_scheme_metadata():
    """
    Fetch raw scheme metadata CSV from AMFI portal.

    Returns:
        str: Raw CSV content or None if failed
    """
    url = API.AMFI_SCHEME_URL
    params = API.AMFI_SCHEME_PARAMS
    logger.info(f"Fetching scheme metadata from {url}")

    try:
        response = requests.get(url, params=params, timeout=API.AMFI_SCHEME_TIMEOUT)
        response.raise_for_status()
        logger.info(f"Received {len(response.content):,} bytes (HTTP {response.status_code})")
        return response.text

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP request failed: {e}")
        return None


def save_metadata_to_r2(csv_content):
    """
    Convert CSV content to Parquet and upload to R2.

    Args:
        csv_content: Raw CSV string from AMFI

    Returns:
        str: R2 path of saved file, or None if failed
    """
    date_str = datetime.now().strftime('%Y%m%d')
    r2 = R2()
    r2_path = r2.get_full_path('metadata', f'scheme_metadata_{date_str}')
    logger.info(f"Target: {r2_path}")

    try:
        conn = r2.setup_connection()

        df = pd.read_csv(StringIO(csv_content))
        logger.info(f"Parsed {len(df):,} rows, {len(df.columns)} columns")

        save_to_parquet(conn, 'scheme_metadata', df, r2_path)
        logger.info(f"Saved to {r2_path}")

        conn.close()
        return r2_path

    except Exception as e:
        logger.error(f"Failed to save metadata to R2: {e}")
        return None


def main():
    """Main function to extract scheme metadata and save to R2."""
    log_script_start(logger, "Scheme Metadata Extractor",
                     "Fetching scheme metadata from AMFI and saving to R2")

    csv_content = fetch_scheme_metadata()
    if not csv_content:
        logger.error("Failed to fetch scheme metadata")
        log_script_end(logger, "Scheme Metadata Extractor", False)
        return 1

    r2_path = save_metadata_to_r2(csv_content)
    success = r2_path is not None

    log_script_end(logger, "Scheme Metadata Extractor", success)
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
