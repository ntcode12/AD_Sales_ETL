import pandas as pd
import numpy as np
import io
import os
from google.cloud import bigquery, storage

# Initialize BigQuery client
bq_client = bigquery.Client()

# BigQuery dataset and table 
PROJECT_ID = os.environ.get('sales-pulse-etl-dashboard')
DATASET_ID = 'sales_data'
TABLE_ID = 'advertising_metrics'

# BigQuery schema
table_schema = [
    bigquery.SchemaField("TV", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Radio", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Newspaper", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Sales", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Total_Spend", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("TV_Percentage", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Radio_Percentage", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Newspaper_Percentage", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("TV_Radio_Interaction", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("TV_Newspaper_Interaction", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Log_Sales", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("Log_TV", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ROAS", "FLOAT", mode="NULLABLE"),
]
def enrich_data(df):
    df['Total_Spend'] = df['TV'] + df['Radio'] + df['Newspaper']
    df['TV_Percentage'] = np.where(df['Total_Spend'] != 0, df['TV'] / df['Total_Spend'], 0)
    df['Radio_Percentage'] = np.where(df['Total_Spend'] != 0, df['Radio'] / df['Total_Spend'], 0)
    df['Newspaper_Percentage'] = np.where(df['Total_Spend'] != 0, df['Newspaper'] / df['Total_Spend'], 0)
    df['TV_Radio_Interaction'] = df['TV'] * df['Radio']
    df['TV_Newspaper_Interaction'] = df['TV'] * df['Newspaper']
    df['Log_Sales'] = np.log(df['Sales'] + 1)
    df['Log_TV'] = np.log(df['TV'] + 1)
    # ROAS (Return on Advertising Spend) metric: Sales / Total_Spend
    # np.where to avoid division by zero
    df['ROAS'] = np.where(df['Total_Spend'] != 0, df['Sales'] / df['Total_Spend'], 0)
    
    return df

def clean_csv(df):
    df = enrich_data(df)
    return df


def load_data_to_bigquery(df):
    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.PARQUET,
        schema=table_schema 
    )
    # Convert DataFrame to Parquet bytes in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    load_job = bq_client.load_table_from_file(
        parquet_buffer,
        table_ref,
        job_config=job_config
    )
    load_job.result()  # Wait for job to complete
    print(f"Loaded {load_job.output_rows} rows into {DATASET_ID}:{TABLE_ID}")

def pull_from_bucket(bucket_name, file_name):
    """Pulls a CSV file from a Cloud Storage bucket, processes it, and loads it into BigQuery."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_bytes()
    
    # Read CSV into DataFrame
    df = pd.read_csv(io.BytesIO(data))
    df = clean_csv(df)
    load_data_to_bigquery(df)
