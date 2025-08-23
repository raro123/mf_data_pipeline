"""
Common storage operations module for R2 and other storage services.
"""
import os
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

class CloudflareR2:
    """Cloudflare R2 storage operations"""
    
    def __init__(self):
        """Initialize R2 client with credentials from environment"""
        self.account_id = os.getenv("R2_ACCOUNT_ID")
        self.endpoint_url = f"https://{self.account_id}.r2.cloudflarestorage.com"
        self.access_key_id = os.getenv("R2_ACCESS_KEY_ID")
        self.secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY")
        self.bucket_name = os.getenv("R2_BUCKET_NAME", "financial-data-store")
        
        self.client = self._get_client()
    
    def _get_client(self):
        """Create and return an S3 client for R2"""
        session = boto3.session.Session()
        return session.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key
        )
    
    def upload_data(self, data_bytes: bytes, object_name: str) -> str:
        """
        Upload data to R2 storage.
        
        Args:
            data_bytes (bytes): The data to upload
            object_name (str): The object name/path in the bucket
            
        Returns:
            str: The R2 path where the data was stored
        """
        logger.info(f"Uploading data to R2 bucket '{self.bucket_name}' as '{object_name}'...")
        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=object_name,
                Body=data_bytes
            )
            r2_path = f"s3a://{self.bucket_name}/{object_name}"
            logger.info(f"Successfully uploaded to R2: {r2_path}")
            return r2_path
            
        except ClientError as e:
            logger.error(f"Error uploading to R2: {e}")
            if e.response['Error']['Code'] == 'NoSuchBucket':
                logger.error(f"R2 bucket '{self.bucket_name}' does not exist. Please create it.")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during R2 upload: {e}")
            raise

def generate_object_name(folder: str, prefix: str, timestamp: datetime, suffix: str = None) -> str:
    """
    Generate a standardized object name for storage.
    
    Args:
        folder (str): Base folder path
        prefix (str): File name prefix
        timestamp (datetime): Timestamp for the file
        suffix (str, optional): Additional identifier to append to filename
        
    Returns:
        str: Generated object name following the pattern:
             {folder}/YYYY/MM/DD/{prefix}_YYYYMMDDHHMMSS[_suffix].csv
    """
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    ts_str = timestamp.strftime("%Y%m%d%H%M%S")
    
    if suffix:
        return f"{folder}/{year}/{month}/{day}/{prefix}_{ts_str}_{suffix}.csv"
    return f"{folder}/{year}/{month}/{day}/{prefix}_{ts_str}.csv"
