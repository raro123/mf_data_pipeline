# File: scripts/ingest_zerodha_mf.py

import pandas as pd
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from datetime import datetime
import os
import io
import boto3
from botocore.exceptions import ClientError
import logging

# --- 1. Configuration (Environment Variables & Constants) ---
# It's best practice to load sensitive info from environment variables
# For local testing, you might use a .env file and `python-dotenv`
# In Prefect, these can be set as Secret blocks or runtime variables.
load_dotenv()  # Load environment variables from .env file if present
KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET") # If applicable
KITE_ACCESS_TOKEN = os.getenv('KITE_ACCESS_TOKEN')
KITE_API_BASE_URL = "https://api.zerodha.com/mf/v1" # Example, confirm from docs
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")  # Set your R2 account ID
R2_ENDPOINT_URL = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = "financial-data-store" # Choose a suitable bucket name
FOLDER_NAME = "raw/zerodha_mf_instruments"  # Folder name in R2 bucket
ZERODHA_DUMP_FILENAME_PREFIX = "zerodha_mf_instruments"

# --- 2. Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 3. Core Functions ---

def authenticate_kite_connect(api_key: str, api_secret: str, access_token = None) -> KiteConnect:


    kite = KiteConnect(api_key=api_key)

    if access_token:
        kite.set_access_token(f'{access_token}')
        logger.info(f'Access token {access_token} loaded from environment')
    else:
        print('Visit this URL and login to get your request token:')
        print(kite.login_url())
        request_token = input('Enter the request token: ')
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data['access_token']
        # Only save to .env if running locally
        if os.path.exists('.env'):
            with open('.env', 'a') as f:
                f.write(f'\nKITE_ACCESS_TOKEN={access_token}\n')
            print('Access token set and saved to .env')
        else:
            logger.info('Access token set (not saved to .env, likely running remotely)')
        kite.set_access_token(access_token)
    return kite

def fetch_zerodha_mf_dump(kite: KiteConnect) -> pd.DataFrame:
    """
    Fetches the mutual fund instrument dump from Zerodha Coin API.
    """
    try:
        mf_instruments = kite.mf_instruments()
        df = pd.DataFrame(mf_instruments)
        return df
    except Exception as e:
        logger.error(f"Error downloading mutual fund instruments: {e}")
        return None



def upload_to_r2(data_bytes: bytes, bucket_name: str, object_name: str) -> str:
    """
    Uploads data bytes to Cloudflare R2.
    """
    session = boto3.session.Session()
    s3_client = session.client(
        's3',
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY
    )
    logger.info(f"Uploading data to R2 bucket '{bucket_name}' as '{object_name}'...")
    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=data_bytes)
        r2_path = f"s3a://{bucket_name}/{object_name}"
        logger.info(f"Successfully uploaded to R2: {r2_path}")
        return r2_path
    except ClientError as e:
        logger.error(f"Error uploading to R2: {e}")
        if e.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f"R2 bucket '{bucket_name}' does not exist. Please create it.")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during R2 upload: {e}")
        raise


def generate_r2_object_name(folder: str, prefix: str, timestamp: datetime) -> str:
    """
    Generates the R2 object name using the specified convention.
    Format: /YYYY/MM/DD/filename_timestamp.csv
    """
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    ts_str = timestamp.strftime("%Y%m%d%H%M%S") # Detailed timestamp for uniqueness
    object_name = f"{folder}/{year}/{month}/{day}/{prefix}_{ts_str}.csv"
    return object_name

# --- 4. Main Ingestion Logic (Callable by Prefect) ---

def ingest_zerodha_mf_dump_workflow():
    """
    Main function to orchestrate fetching Zerodha MF dump and uploading to R2.
    This function will be wrapped by a Prefect flow.
    """
    current_timestamp = datetime.now()
    logger.info(f"Starting Zerodha MF ingestion at {current_timestamp}...")

    # Step 1: Authenticate
    kite = authenticate_kite_connect(KITE_API_KEY, KITE_API_SECRET, KITE_ACCESS_TOKEN)
    if not kite:
        logger.error("Failed to authenticate with Kite Connect. Exiting.")
        return None
    logger.info("Authenticated with Kite Connect successfully.")

    # Step 2: Fetch data
    mf_df =  fetch_zerodha_mf_dump(kite)
    if mf_df is None:
        logger.error("Failed to fetch mutual fund instruments. Exiting.")
        return None
    logger.info(f"Fetched {len(mf_df)} mutual fund instruments from Zerodha.")
    # Check if DataFrame is empty
    if mf_df is None or mf_df.empty:
        logger.warning("No MF instruments fetched. Skipping R2 upload.")
        return None

    # Step 3: Convert to CSV bytes
    csv_buffer = io.StringIO()
    mf_df.to_csv(csv_buffer, index=False)
    mf_df.to_csv("raw/zerodha_mf_meta"+ZERODHA_DUMP_FILENAME_PREFIX+".csv", index=False)  # Save a local copy for reference
    csv_bytes = csv_buffer.getvalue().encode('utf-8')

    # Step 4: Generate R2 object name
    object_name = generate_r2_object_name(FOLDER_NAME, ZERODHA_DUMP_FILENAME_PREFIX, current_timestamp)

    # Step 5: Upload to R2
    r2_path = upload_to_r2(csv_bytes, R2_BUCKET_NAME, object_name)

    logger.info(f"Zerodha MF ingestion completed. Data available at: {r2_path}")
    return r2_path # Return the path for Prefect's metadata/observability


if __name__ == "__main__":
    # For local testing outside Prefect
    # IMPORTANT: Set environment variables first for local testing!
    # e.g., export ZERODHA_API_KEY="your_key"
    # export R2_ACCESS_KEY_ID="your_r2_key"
    # export R2_SECRET_ACCESS_KEY="your_r2_secret"
    # export R2_ENDPOINT_URL="https://<your_account_id>.r2.cloudflarestorage.com"
    
    # You might want to use python-dotenv for local development:
    # from dotenv import load_dotenv
    # load_dotenv()

    try:
        ingest_zerodha_mf_dump_workflow()
    except Exception as e:
        logger.exception("Zerodha MF ingestion failed during local run.")