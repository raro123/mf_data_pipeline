"""
Centralized logging setup for the Mutual Fund Data Pipeline.

This module provides a consistent logging configuration across all scripts,
eliminating code duplication and ensuring uniform log formatting.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add project root to Python path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.settings import Logging, get_log_file_path


def setup_logger(
    name: str,
    log_pattern: str,
    level: str = None,
    console: bool = True,
    file_logging: bool = True,
    date_str: Optional[str] = None
) -> logging.Logger:
    """
    Set up a logger with consistent configuration.
    
    Args:
        name: Logger name (usually __name__)
        log_pattern: Log filename pattern from config.settings.Logging
        level: Log level (INFO, DEBUG, WARNING, ERROR)
        console: Enable console output
        file_logging: Enable file output
        date_str: Date string for log filename (defaults to today)
        
    Returns:
        logging.Logger: Configured logger instance
        
    Example:
        >>> from utils.logging_setup import setup_logger
        >>> from config.settings import Logging
        >>> 
        >>> logger = setup_logger(__name__, Logging.FETCH_DAILY_LOG)
        >>> logger.info("Daily NAV processing started")
    """
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level or Logging.LOG_LEVEL))
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(Logging.LOG_FORMAT)
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level or Logging.LOG_LEVEL))
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if file_logging:
        log_file_path = get_log_file_path(log_pattern, date_str)
        
        # Ensure log directory exists
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setLevel(getattr(logging, level or Logging.LOG_LEVEL))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"📝 Logging to: {log_file_path}")
    
    return logger


def log_script_start(logger: logging.Logger, script_name: str, description: str = ""):
    """
    Log standardized script start message.
    
    Args:
        logger: Logger instance
        script_name: Name of the script
        description: Brief description of what the script does
    """
    logger.info("=" * 60)
    logger.info(f"🚀 Starting {script_name}")
    if description:
        logger.info(f"📝 {description}")
    logger.info("=" * 60)


def log_script_end(logger: logging.Logger, script_name: str, success: bool = True):
    """
    Log standardized script end message.
    
    Args:
        logger: Logger instance  
        script_name: Name of the script
        success: Whether script completed successfully
    """
    status = "✅ COMPLETED" if success else "❌ FAILED"
    logger.info("=" * 60)
    logger.info(f"{status}: {script_name}")
    logger.info("=" * 60)


def log_data_summary(logger: logging.Logger, df, data_type: str = "data"):
    """
    Log standardized data summary.
    
    Args:
        logger: Logger instance
        df: pandas DataFrame
        data_type: Type of data being summarized
    """
    if df is None or df.empty:
        logger.warning(f"⚠️ No {data_type} to summarize")
        return
    
    logger.info(f"📊 {data_type.title()} Summary:")
    logger.info(f"   Records: {len(df):,}")
    logger.info(f"   Columns: {len(df.columns)}")
    
    if hasattr(df, 'memory_usage'):
        memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
        logger.info(f"   Memory: {memory_mb:.2f} MB")


def log_file_operation(logger: logging.Logger, operation: str, file_path: Path, 
                      success: bool = True, size_mb: float = None):
    """
    Log standardized file operation.
    
    Args:
        logger: Logger instance
        operation: Type of operation (saved, loaded, created, etc.)
        file_path: Path to the file
        success: Whether operation was successful  
        size_mb: File size in MB
    """
    status = "✅" if success else "❌"
    size_info = f" ({size_mb:.2f} MB)" if size_mb else ""
    
    logger.info(f"{status} {operation.title()}: {file_path}{size_info}")


def log_validation_results(logger: logging.Logger, results: dict):
    """
    Log standardized validation results.
    
    Args:
        logger: Logger instance
        results: Dictionary with validation results
        
    Example:
        results = {
            'total_records': 1000,
            'valid_records': 995, 
            'null_values': 5,
            'duplicates': 0,
            'invalid_dates': 2
        }
    """
    logger.info("🔍 Validation Results:")
    
    for key, value in results.items():
        if isinstance(value, (int, float)):
            logger.info(f"   {key.replace('_', ' ').title()}: {value:,}")
        else:
            logger.info(f"   {key.replace('_', ' ').title()}: {value}")


def cleanup_old_logs(retention_days: int = None):
    """
    Clean up old log files based on retention policy.
    
    Args:
        retention_days: Number of days to retain logs (from config if not provided)
    """
    from config.settings import Paths, Logging
    
    retention_days = retention_days or Logging.LOG_RETENTION_DAYS
    cutoff_date = datetime.now().timestamp() - (retention_days * 24 * 60 * 60)
    
    if not Paths.LOGS.exists():
        return
    
    deleted_count = 0
    for log_file in Paths.LOGS.glob("*.log"):
        if log_file.stat().st_mtime < cutoff_date:
            log_file.unlink()
            deleted_count += 1
    
    if deleted_count > 0:
        print(f"🧹 Cleaned up {deleted_count} old log files")


# =============================================================================
# PRE-CONFIGURED LOGGERS FOR EACH SCRIPT
# =============================================================================

def get_historical_fetch_logger(name: str = __name__) -> logging.Logger:
    """Get logger for historical NAV fetching."""
    return setup_logger(name, Logging.FETCH_HISTORICAL_LOG)


def get_historical_clean_logger(name: str = __name__) -> logging.Logger:
    """Get logger for historical NAV cleaning."""
    return setup_logger(name, Logging.CLEAN_HISTORICAL_LOG)


def get_daily_fetch_logger(name: str = __name__) -> logging.Logger:
    """Get logger for daily NAV fetching."""
    return setup_logger(name, Logging.FETCH_DAILY_LOG)


def get_combine_table_logger(name: str = __name__) -> logging.Logger:
    """Get logger for combined table creation."""
    return setup_logger(name, Logging.COMBINE_TABLE_LOG)


def get_extract_metadata_logger(name: str = __name__) -> logging.Logger:
    """Get logger for scheme metadata extraction."""
    return setup_logger(name, Logging.EXTRACT_METADATA_LOG)


def get_clean_metadata_logger(name: str = __name__) -> logging.Logger:
    """Get logger for scheme metadata cleaning."""
    return setup_logger(name, Logging.CLEAN_METADATA_LOG)


if __name__ == "__main__":
    # Test logging setup
    test_logger = setup_logger(__name__, "test_{date}.log")
    
    log_script_start(test_logger, "Logging Test", "Testing centralized logging system")
    
    test_logger.info("This is a test message")
    test_logger.warning("This is a warning")
    test_logger.error("This is an error")
    
    # Test data summary with mock data
    import pandas as pd
    test_df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
    log_data_summary(test_logger, test_df, "test data")
    
    log_script_end(test_logger, "Logging Test", True)
    
    print("✅ Logging test completed - check logs/ directory")