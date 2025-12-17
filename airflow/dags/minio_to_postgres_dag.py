from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import boto3
from sqlalchemy import create_engine
from pathlib import Path
import logging

# -----------------------------
# CONFIG
# -----------------------------
MINIO_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET = "raw-data"
FILE_NAME = "large_dataset.csv"
LOCAL_FILE = Path("/tmp/data.csv")

POSTGRES_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/analytics"

# -----------------------------
# FUNCTIONS
# -----------------------------
def extract_from_minio(**kwargs):
    """Download CSV from MinIO to local path inside Airflow container"""
    logging.info("Starting extraction from MinIO...")
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )
        s3.download_file(BUCKET, FILE_NAME, str(LOCAL_FILE))
        logging.info(f"File downloaded successfully to {LOCAL_FILE}")
    except Exception as e:
        logging.error(f"Error downloading file from MinIO: {e}")
        raise

def clean_transform_load(**kwargs):
    """Clean CSV, apply transformations, aggregate ratings per movie, write to PostgreSQL"""
    logging.info("Starting cleaning and transformation...")
    
    if not LOCAL_FILE.exists():
        raise FileNotFoundError(f"{LOCAL_FILE} does not exist!")

    df = pd.read_csv(LOCAL_FILE)
    logging.info(f"Initial rows: {len(df)}")

    # Clean data
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)
    logging.info(f"Rows after dropna & drop_duplicates: {len(df)}")

    # Outlier removal (IQR)
    if 'rating' in df.columns and len(df) > 10:
        Q1 = df['rating'].quantile(0.25)
        Q3 = df['rating'].quantile(0.75)
        IQR = Q3 - Q1
        df = df[(df['rating'] >= Q1 - 1.5 * IQR) & (df['rating'] <= Q3 + 1.5 * IQR)]
        logging.info(f"Rows after IQR filtering: {len(df)}")

    # Aggregate ratings per movie
    agg = df.groupby('movieId').agg(
        avg_rating=('rating', 'mean'),
        max_rating=('rating', 'max'),
        rating_count=('rating', 'count')
    ).reset_index()

    # Rename column to match table
    agg.rename(columns={'movieId': 'movie_id'}, inplace=True)

    # Connect to PostgreSQL
    try:
        engine = create_engine(POSTGRES_CONN)
        agg.to_sql("analytics_ratings", engine, if_exists='replace', index=False)
        logging.info("Data written to PostgreSQL successfully!")
    except Exception as e:
        logging.error(f"Error writing to PostgreSQL: {e}")
        raise

# -----------------------------
# DAG DEFINITION
# -----------------------------
with DAG(
    dag_id="minio_to_postgres_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bigdata", "etl", "minio", "postgres"]
) as dag:

    t1 = PythonOperator(
        task_id="extract_from_minio",
        python_callable=extract_from_minio,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id="clean_transform_load",
        python_callable=clean_transform_load,
        provide_context=True
    )

    t1 >> t2
