#!/usr/bin/env python3

# Test script to verify parallel processing works with a few files
import sys
sys.path.append('.')

from scripts.clean_nav_data import combine_all_nav_files
import logging

# Setup simple logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Test with just a few files
output_file = combine_all_nav_files(output_file="raw/amfi_nav_history/test_historical_nav_data.parquet")
print(f"Test completed. Output: {output_file}")