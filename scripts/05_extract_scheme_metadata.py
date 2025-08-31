#!/usr/bin/env python3
"""
Scheme Metadata Extractor

Fetches scheme metadata from AMFI portal and saves as raw CSV.
This script has been refactored to use centralized configuration and logging.
"""

import requests
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from io import StringIO

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, API
from utils.logging_setup import get_extract_metadata_logger, log_script_start, log_script_end, log_file_operation

# Initialize logger
logger = get_extract_metadata_logger(__name__)

def fetch_scheme_metadata():
    """
    Fetch raw scheme metadata from AMFI portal.
    
    Returns:
        str: Raw CSV content or None if failed
    """
    logger.info("📡 Fetching scheme metadata from AMFI portal...")
    
    # Use configured API settings
    url = API.AMFI_SCHEME_URL
    params = API.AMFI_SCHEME_PARAMS
    
    try:
        logger.info(f"🌐 Requesting: {url}")
        logger.info(f"📋 Parameters: {params}")
        
        response = requests.get(url, params=params, timeout=API.AMFI_SCHEME_TIMEOUT)
        response.raise_for_status()
        
        logger.info(f"✅ HTTP {response.status_code}: {len(response.content):,} bytes received")
        logger.info(f"📄 Content-Type: {response.headers.get('Content-Type', 'Unknown')}")
        
        # Basic validation of CSV content
        csv_content = response.text
        lines = csv_content.split('\n')
        logger.info(f"📊 Total lines: {len(lines):,}")
        
        # Check if it looks like CSV
        if len(lines) > 1 and ',' in lines[0]:
            header = lines[0]
            logger.info(f"📋 CSV Header: {header[:100]}...")
            logger.info(f"📊 Data lines: {len(lines) - 1:,}")
        else:
            logger.warning("⚠️ Content doesn't appear to be CSV format")
        
        return csv_content
        
    except requests.exceptions.Timeout:
        logger.error("❌ Request timeout - server took too long to respond")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ HTTP request failed: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error during fetch: {e}")
        return None

def save_raw_metadata(csv_content):
    """
    Save raw CSV content to file using configured paths.
    
    Args:
        csv_content (str): Raw CSV content
        
    Returns:
        str: Path to saved file or None if failed
    """
    # Use configured output path
    output_file = Paths.SCHEME_METADATA_RAW
    
    logger.info(f"💾 Saving raw metadata to {output_file}...")
    
    if not csv_content:
        logger.error("No content to save")
        return None
    
    # Create output directory
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Save raw CSV content
        # Use configured encoding
        from config.settings import Processing
        with open(output_file, 'w', encoding=Processing.CSV_ENCODING) as f:
            f.write(csv_content)
        
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", output_file, True, file_size_mb)
        
        # Quick validation - try to read as CSV
        try:
            df_test = pd.read_csv(output_file, nrows=5)
            logger.info(f"📋 Columns detected: {len(df_test.columns)}")
            logger.info(f"📊 Sample shape: {df_test.shape}")
        except Exception as e:
            logger.warning(f"⚠️ CSV validation warning: {e}")
        
        return str(output_file)
        
    except Exception as e:
        logger.error(f"Failed to save raw metadata: {e}")
        return None

def main():
    """Main function to extract and save raw scheme metadata."""
    
    log_script_start(logger, "Scheme Metadata Extractor", 
                    "Fetching raw scheme metadata from AMFI portal")
    
    # Ensure directories exist
    Paths.create_directories()
    
    # Fetch raw data
    csv_content = fetch_scheme_metadata()
    if not csv_content:
        logger.error("❌ Failed to fetch scheme metadata")
        log_script_end(logger, "Scheme Metadata Extractor", False)
        return 1
    
    # Save raw data
    saved_path = save_raw_metadata(csv_content)
    success = saved_path is not None
    
    if success:
        logger.info("➡️  Next step: run 06_clean_scheme_metadata.py to process this data")
    
    log_script_end(logger, "Scheme Metadata Extractor", success)
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)