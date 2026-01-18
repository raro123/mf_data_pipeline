"""
Central configuration for Mutual Fund Data Pipeline.

This module contains all configurable settings, paths, URLs, and constants
used across the pipeline scripts. Modify values here instead of hardcoding
them in individual scripts.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb

# Load environment variables
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# =============================================================================
# DIRECTORY PATHS
# =============================================================================

class Paths:
    """All file and directory paths used in the pipeline."""
    
    # Base directories
    DATA_ROOT = PROJECT_ROOT / "data"
    RAW_DATA = DATA_ROOT / "raw"
    PROCESSED_DATA = DATA_ROOT / "processed"
    LOGS = PROJECT_ROOT / "logs"
    SCRIPTS = PROJECT_ROOT / "scripts"
    
    # Raw data directories
    RAW_NAV_CSV = RAW_DATA / "nav_historical"  # Raw CSV files from API
    RAW_NAV_DAILY = RAW_DATA / "nav_daily"
    SCHEME_METADATA_DIR = RAW_DATA / "scheme_metadata"
    SCHEME_METADATA_RAW = SCHEME_METADATA_DIR / "scheme_metadata_raw.csv"
    
    # Processed data directories
    RAW_NAV_HISTORICAL = PROCESSED_DATA / "nav_historical"  # Cleaned historical batches
    PROCESSED_NAV_DAILY = PROCESSED_DATA / "nav_daily"
    NAV_COMBINED = PROCESSED_DATA / "nav_combined"
    PROCESSED_SCHEME_METADATA = PROCESSED_DATA / "scheme_metadata"
    ANALYTICAL = PROCESSED_DATA / "analytical"
    
    # Specific file paths
    SCHEME_METADATA_CLEAN = PROCESSED_SCHEME_METADATA / "amfi_scheme_metadata.parquet"
    SCHEME_METADATA_CSV = PROCESSED_SCHEME_METADATA / "amfi_scheme_metadata.csv"
    SCHEME_MASTERDATA = PROCESSED_SCHEME_METADATA / "scheme_masterdata.parquet"
    SCHEME_MASTERDATA_CSV = PROCESSED_SCHEME_METADATA / "scheme_masterdata.csv"
    COMBINED_NAV_TABLE = NAV_COMBINED / "raw_nav_table.parquet"
    
    # Create all directories
    @classmethod
    def create_directories(cls):
        """Create all required directories if they don't exist."""
        directories = [
            cls.DATA_ROOT,
            cls.RAW_DATA,
            cls.PROCESSED_DATA, 
            cls.LOGS,
            cls.RAW_NAV_CSV,
            cls.RAW_NAV_DAILY,
            cls.SCHEME_METADATA_DIR,
            cls.RAW_NAV_HISTORICAL,
            cls.PROCESSED_NAV_DAILY,
            cls.NAV_COMBINED,
            cls.PROCESSED_SCHEME_METADATA,
            cls.ANALYTICAL
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            
# =============================================================================
# R2 CONFIGURATION
# =============================================================================

BUCKET_NAME = 'financial-data-store'
ASSET_CLASS = 'mutual_funds'


class R2:
    """Cloud storage (R2) configuration."""
    def __init__(self,bucket_name=BUCKET_NAME,asset_class= ASSET_CLASS):
        self.bucket_name = bucket_name
        self.asset_class = asset_class
    
    def get_full_path(self,clean_raw, file_name,file_extension='parquet'):
        return f"r2://{self.bucket_name}/{self.asset_class}/{clean_raw}/{file_name}.{file_extension}"
    
    # R2 Credentials from environment variables
    ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
    SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
    ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
    
    def setup_connection(self):
        con = duckdb.connect()
        con.install_extension('httpfs')
        con.load_extension('httpfs')
        """Setup R2 connection using environment variables."""
        con.sql(f"""
            CREATE OR REPLACE SECRET (
        TYPE r2,
        KEY_ID '{self.ACCESS_KEY_ID}',
        SECRET '{self.SECRET_ACCESS_KEY}',
        ACCOUNT_ID '{self.ACCOUNT_ID}'
    );
        """)
        return con


# =============================================================================
# API CONFIGURATION
# =============================================================================

class API:
    """API endpoints, parameters, and request configuration."""
    
    # AMFI NAV API
    AMFI_NAV_BASE_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
    AMFI_NAV_HISTORY_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
    AMFI_NAV_TIMEOUT = int(os.getenv("AMFI_NAV_TIMEOUT", "30"))
    
    # AMFI Scheme Metadata API  
    AMFI_SCHEME_URL = "https://portal.amfiindia.com/DownloadSchemeData_Po.aspx"
    AMFI_SCHEME_PARAMS = {"mf": "0"}
    AMFI_SCHEME_TIMEOUT = int(os.getenv("AMFI_SCHEME_TIMEOUT", "30"))
    
    # Request configuration
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))  # seconds

# =============================================================================
# PROCESSING CONFIGURATION  
# =============================================================================

class Processing:
    """Data processing parameters and batch sizes."""
    
    # Historical data processing
    HISTORICAL_FETCH_DAYS = int(os.getenv("HISTORICAL_FETCH_DAYS", "90"))
    HISTORICAL_BATCH_SIZE = int(os.getenv("HISTORICAL_BATCH_SIZE", "15"))
    
    # Memory management
    CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))
    MAX_MEMORY_GB = float(os.getenv("MAX_MEMORY_GB", "2.0"))
    
    # File format settings
    PARQUET_COMPRESSION = os.getenv("PARQUET_COMPRESSION", "snappy")
    CSV_ENCODING = os.getenv("CSV_ENCODING", "utf-8")

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

class Logging:
    """Logging configuration and patterns."""
    
    # Log levels
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    # Log file patterns
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    
    # Log file naming patterns
    FETCH_HISTORICAL_LOG = "nav_fetch_{date}.log"
    CLEAN_HISTORICAL_LOG = "clean_nav_{date}.log" 
    FETCH_DAILY_LOG = "daily_nav_{date}.log"
    COMBINE_TABLE_LOG = "raw_nav_table_{date}.log"
    EXTRACT_METADATA_LOG = "extract_scheme_metadata_{date}.log"
    CLEAN_METADATA_LOG = "clean_scheme_metadata_{date}.log"
    ANALYTICAL_NAV_LOG = "analytical_nav_{date}.log"
    
    # Log retention
    LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "30"))

# =============================================================================
# DATA VALIDATION RULES
# =============================================================================

class Validation:
    """Data validation rules and thresholds."""
    
    # NAV data validation
    MIN_NAV_VALUE = float(os.getenv("MIN_NAV_VALUE", "0.01"))
    MAX_NAV_VALUE = float(os.getenv("MAX_NAV_VALUE", "10000.0"))
    
    # Scheme validation
    MIN_SCHEME_CODE = int(os.getenv("MIN_SCHEME_CODE", "100000"))
    MAX_SCHEME_CODE = int(os.getenv("MAX_SCHEME_CODE", "999999"))
    
    # Date validation
    MIN_LAUNCH_YEAR = int(os.getenv("MIN_LAUNCH_YEAR", "1990"))
    MAX_LAUNCH_YEAR = int(os.getenv("MAX_LAUNCH_YEAR", "2030"))
    
    # Quality thresholds
    MAX_NULL_PERCENTAGE = float(os.getenv("MAX_NULL_PERCENTAGE", "0.1"))  # 10%
    MAX_DUPLICATE_PERCENTAGE = float(os.getenv("MAX_DUPLICATE_PERCENTAGE", "0.05"))  # 5%

# =============================================================================
# ENVIRONMENT SETTINGS
# =============================================================================

class Environment:
    """Environment-specific settings."""
    
    ENV = os.getenv("ENVIRONMENT", "development")
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"
    
    # Development settings
    SAMPLE_MODE = os.getenv("SAMPLE_MODE", "False").lower() == "true"
    SAMPLE_SIZE = int(os.getenv("SAMPLE_SIZE", "1000"))
    
    # Production settings
    ENABLE_MONITORING = os.getenv("ENABLE_MONITORING", "False").lower() == "true"
    ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")

# =============================================================================
# DERIVED SETTINGS
# =============================================================================

def get_log_file_path(log_pattern: str, date_str: str = None) -> Path:
    """
    Generate log file path based on pattern and date.
    
    Args:
        log_pattern: Log filename pattern from Logging class
        date_str: Date string (defaults to today)
        
    Returns:
        Path: Complete log file path
    """
    from datetime import datetime
    
    if date_str is None:
        date_str = datetime.now().strftime('%Y%m%d')
    
    filename = log_pattern.format(date=date_str)
    return Paths.LOGS / filename

def get_batch_file_path(batch_num: int) -> Path:
    """Generate path for historical batch files."""
    return Paths.RAW_NAV_HISTORICAL / f"batch_{batch_num:02d}.parquet"

def get_daily_nav_file_path(date_str: str) -> Path:
    """Generate path for daily NAV files."""
    return Paths.RAW_NAV_DAILY / f"daily_nav_{date_str}.parquet"

def get_timestamped_metadata_file_path(date_str: str = None) -> Path:
    """Generate path for timestamped raw metadata files."""
    from datetime import datetime
    
    if date_str is None:
        date_str = datetime.now().strftime('%Y%m%d')
    
    return Paths.SCHEME_METADATA_DIR / f"scheme_metadata_{date_str}.csv"

def get_latest_raw_metadata_file() -> Path:
    """
    Find and return the most recent raw metadata file.
    
    Returns:
        Path: Path to the latest raw metadata file
        
    Raises:
        FileNotFoundError: If no metadata files found
    """
    metadata_dir = Paths.SCHEME_METADATA_DIR
    
    if not metadata_dir.exists():
        raise FileNotFoundError(f"Metadata directory not found: {metadata_dir}")
    
    # Look for timestamped files first
    timestamped_files = list(metadata_dir.glob("scheme_metadata_*.csv"))
    
    if timestamped_files:
        # Return the most recently modified timestamped file
        return max(timestamped_files, key=lambda f: f.stat().st_mtime)
    
    # Fallback to legacy filename
    legacy_file = Paths.SCHEME_METADATA_RAW
    if legacy_file.exists():
        return legacy_file
        
    raise FileNotFoundError(f"No raw metadata files found in {metadata_dir}")

def should_process_metadata() -> bool:
    """
    Check if raw metadata is newer than processed metadata.
    
    Returns:
        bool: True if processing is needed, False otherwise
    """
    try:
        latest_raw = get_latest_raw_metadata_file()
        processed_file = Paths.SCHEME_METADATA_CLEAN
        
        # Process if processed file doesn't exist
        if not processed_file.exists():
            return True
            
        # Process if raw file is newer than processed file
        return latest_raw.stat().st_mtime > processed_file.stat().st_mtime
        
    except FileNotFoundError:
        return False

# =============================================================================
# INITIALIZATION
# =============================================================================

def initialize_project():
    """Initialize project by creating necessary directories."""
    Paths.create_directories()
    print(f"✅ Project initialized")
    print(f"📁 Project root: {PROJECT_ROOT}")
    print(f"🔧 Environment: {Environment.ENV}")
    print(f"📊 Debug mode: {Environment.DEBUG}")

if __name__ == "__main__":
    initialize_project()