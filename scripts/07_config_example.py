#!/usr/bin/env python3
"""
Example script showing how to use the centralized configuration system.

BEFORE refactoring:
- Hardcoded paths everywhere
- Duplicate logging setup
- Magic numbers scattered
- No environment customization

AFTER refactoring:
- All settings from config
- Centralized logging
- Environment variables
- Clean, maintainable code
"""

import sys
from pathlib import Path
import pandas as pd

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, API, Processing, Validation
from utils.logging_setup import get_daily_fetch_logger, log_script_start, log_script_end, log_data_summary

def main():
    """Demonstrate configuration usage."""
    
    # BEFORE: Hardcoded logging setup
    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # logger = logging.getLogger(__name__)
    
    # AFTER: Centralized logging
    logger = get_daily_fetch_logger(__name__)
    
    log_script_start(logger, "Configuration Demo", "Showing before vs after refactoring")
    
    # BEFORE: Hardcoded paths
    # output_file = "raw/amfi_nav_daily/daily_nav_20250831.parquet"
    # input_dir = "raw/amfi_nav_history"
    
    # AFTER: Configuration-based paths
    daily_file = Paths.RAW_NAV_DAILY / "daily_nav_20250831.parquet"
    history_dir = Paths.RAW_NAV_HISTORICAL
    
    logger.info("📁 Using configured paths:")
    logger.info(f"   Daily NAV dir: {Paths.RAW_NAV_DAILY}")
    logger.info(f"   Historical dir: {Paths.RAW_NAV_HISTORICAL}")
    logger.info(f"   Logs dir: {Paths.LOGS}")
    
    # BEFORE: Magic numbers
    # timeout = 30
    # batch_size = 15
    # min_nav = 0.01
    
    # AFTER: Configuration constants
    timeout = API.AMFI_NAV_TIMEOUT
    batch_size = Processing.HISTORICAL_BATCH_SIZE
    min_nav = Validation.MIN_NAV_VALUE
    
    logger.info("⚙️ Using configured values:")
    logger.info(f"   API timeout: {timeout}s")
    logger.info(f"   Batch size: {batch_size}")
    logger.info(f"   Min NAV value: ₹{min_nav}")
    
    # BEFORE: Manual directory creation
    # os.makedirs("raw/amfi_nav_daily", exist_ok=True)
    # os.makedirs("logs", exist_ok=True)
    
    # AFTER: Centralized directory creation
    Paths.create_directories()
    logger.info("✅ All directories created/verified")
    
    # Demo: Create some sample data and log it
    sample_data = pd.DataFrame({
        'scheme_code': [123456, 123457, 123458],
        'nav': [10.50, 25.30, 8.75],
        'date': pd.date_range('2025-01-01', periods=3)
    })
    
    log_data_summary(logger, sample_data, "sample NAV data")
    
    # Demo: Show environment-aware behavior
    from config.settings import Environment
    
    if Environment.DEBUG:
        logger.debug("Debug mode enabled - showing detailed info")
    
    if Environment.SAMPLE_MODE:
        logger.info(f"Sample mode: processing only {Environment.SAMPLE_SIZE} records")
    
    log_script_end(logger, "Configuration Demo", True)

if __name__ == "__main__":
    main()