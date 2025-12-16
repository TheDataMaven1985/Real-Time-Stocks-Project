import os
import boto3
import snowflake.connector                  
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Minio configuration
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "bronze-stocks-transaction"
LOCAL_DIR = "/tmp/minio_data/"

# Snowflake configuration
SNOWFLAKE_USER = "<<YOUR_USERNAME>>"
SNOWFLAKE_PASSWORD = "<<YOUR_PASSWORD>>"
SNOWFLAKE_ACCOUNT = "<<YOUR_ACCOUNT>>"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = "STOCKS_DB"
SNOWFLAKE_SCHEMA = "COMMON"

# Download files from Minio
def download_from_minio():
    """Downloads all files from the specified Minio bucket to a local directory."""
    try:
        # Create file directory
        os.makedirs(LOCAL_DIR, exist_ok=True)
        
        # Connect to Minio
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        # List and download objects
        response = s3.list_objects_v2(Bucket=BUCKET)

        if 'Contents' not in response:                         # Check if bucket is empty
            print("No files found in the bucket.")
            return []
        objects = response['Contents']
        print(f"Found {len(objects)} files in the bucket.")

        local_files = []                                       # Dowlnload files in local directory
        for obj in objects:
            key = obj['Key']
            local_path = os.path.join(LOCAL_DIR, os.path.basename(key))

            try:
                s3.download_file(BUCKET, key, local_path)
                print(f"Downloaded {key} to {local_path}")
                local_files.append(local_path)
            except Exception as e:
                print(f"Error downloading {key}: {e}")
        
        print(f" Successfully downlaoded {len(local_files)} files.")
        return local_files
    
    except Exception as e:
        print(f"Error in download_from_minio: {e}")

# Load data into Snowflake
def load_into_snowflake(**context):
    """Loads downloaded files into Snowflake table."""

    try:
        # Get downloaded files from previous task
        local_files = context['ti'].xcom_pull(task_ids='download_from_minio')

        if not local_files:
            print("No files to load into Snowflake.")
            return

        print(f"Received {len(local_files)} files from previous task.")

        # Connect to Snowflake and load data
        print("Cnnecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cur = conn.cursor()
        print("Connected to Snowflake.")

        # Upload files into Snowflake stage
        upload_count = 0
        for f in local_files:
            try:
                cur.execute(f"""PUT file://{f} @%bronze_stock_quotes_raw""")
                print("Uploaded file to Snowflake stage:", f)
                upload_count += 1
            except Exception as e:
                print(f"Error uploading {f} to Snowflake stage: {e}")

        print(f"Successfully uploaded {upload_count} files to Snowflake stage.")

        # Copy data into Snowflake table
        print("Loading data into Snowflake table...")
        cur.execute("""
            COPY INTO bronze_stock_quotes_raw
            FROM @%bronze_stock_quotes_raw
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE'
        """)

        # Get copy results
        result = cur.fetchone()
        print(f"COPY INTO completed")
        print(f"Files loaded: {result[0] if result else 'Unknown'}")
        print(f"Rows inserted: {result[2] if result else 'Unknown'}")
        
        # Cleanup: Remove files from stage
        cur.execute("REMOVE @%bronze_stock_quotes_raw")
        print("Cleaned up stage")

        cur.close()
        conn.close()
        print("Snowflake connection closed.")

    except Exception as e:
        print(f"Error in load_into_snowflake: {e}")

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 14),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# define the DAG
with DAG(
    "minio_to_snowflake",
    default_args=default_args,
    description="ETL pipeline from Minio to Snowflake",
    schedule_interval="*/5 * * * *",                      # every 5 minutes
    catchup=False,
    tags=["minio", "snowflake"],
) as dag:
    
    # Task 1: Download files from Minio
    t1 = PythonOperator(
        task_id='download_from_minio',
        python_callable=download_from_minio
    )

    # Task 2: Load files into Snowflake
    t2 = PythonOperator(
        task_id='load_into_snowflake',
        python_callable=load_into_snowflake,
        provide_context=True
    )

    # Define task dependencies
    t1 >> t2