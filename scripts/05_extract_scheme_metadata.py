#!/usr/bin/env python3

import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from io import StringIO
from dotenv import load_dotenv

# Configuration
load_dotenv()

# Logging setup
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/extract_scheme_metadata_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_scheme_metadata():
    """
    Fetch raw scheme metadata from AMFI portal.
    
    Returns:
        str: Raw CSV content or None if failed
    """
    logger.info("📡 Fetching scheme metadata from AMFI portal...")
    
    url = 'https://portal.amfiindia.com/DownloadSchemeData_Po.aspx'
    params = {'mf': '0'}
    
    try:
        logger.info(f"🌐 Requesting: {url}")
        logger.info(f"📋 Parameters: {params}")
        
        response = requests.get(url, params=params, timeout=30)
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

def save_raw_metadata(csv_content, output_path="raw/scheme_metadata/scheme_metadata_raw.csv"):
    """
    Save raw CSV content to file.
    
    Args:
        csv_content (str): Raw CSV content
        output_path (str): Output file path
        
    Returns:
        str: Path to saved file or None if failed
    """
    logger.info(f"💾 Saving raw metadata to {output_path}...")
    
    if not csv_content:
        logger.error("No content to save")
        return None
    
    # Create output directory
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Save raw CSV content
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        
        logger.info(f"✅ Saved raw metadata: {output_file}")
        logger.info(f"📦 Size: {file_size_mb:.2f} MB")
        
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
    logger.info("🚀 Starting scheme metadata extraction...")
    logger.info("📝 This script only extracts raw data - run 06_clean_scheme_metadata.py next")
    
    # Fetch raw data
    csv_content = fetch_scheme_metadata()
    if not csv_content:
        logger.error("❌ Failed to fetch scheme metadata")
        return 1
    
    # Save raw data
    saved_path = save_raw_metadata(csv_content)
    if saved_path:
        logger.info(f"🎉 Successfully extracted raw scheme metadata: {saved_path}")
        logger.info("➡️  Next step: run 06_clean_scheme_metadata.py to process this data")
        return 0
    else:
        logger.error("❌ Failed to save raw metadata")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)