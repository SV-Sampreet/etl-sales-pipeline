from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl.extract import extract_raw_data
from etl.transform import transform_data, save_clean_data, save_rejected_data
from etl.load import load_data_from_file

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define paths
BASE_DIR = Path(__file__).parent.parent
INPUT_PATH = str(BASE_DIR / 'data' / 'raw' / 'sales_data.csv')
CLEAN_OUTPUT_PATH = str(BASE_DIR / 'data' / 'processed' / 'sales_clean.csv')
REJECTED_OUTPUT_PATH = str(BASE_DIR / 'data' / 'rejected' / 'sales_rejected.csv')
DB_PATH = str(BASE_DIR / 'data' / 'sales.db')
SCHEMA_PATH = str(BASE_DIR / 'sql' / 'schema.sql')


def extract_task(**context):
    """Extract raw data from CSV."""
    print("Starting extraction task")
    df = extract_raw_data(INPUT_PATH)
    print(f"Extracted {len(df)} rows")
    # Store row count in XCom for downstream tasks
    context['ti'].xcom_push(key='extraction_rows', value=len(df))
    return INPUT_PATH


def transform_task(**context):
    """Transform and validate data."""
    print("Starting transformation task")
    from etl.extract import extract_raw_data

    # Extract data (or could receive from previous task via file)
    df_raw = extract_raw_data(INPUT_PATH)

    # Transform data
    df_clean, df_rejected, metrics = transform_data(df_raw)

    # Save outputs
    save_clean_data(df_clean, CLEAN_OUTPUT_PATH)
    save_rejected_data(df_rejected, REJECTED_OUTPUT_PATH)

    print(f"Transformation completed. Accepted: {metrics['accepted_count']}, "
          f"Rejected: {metrics['rejected_count']}, Duplicates: {metrics['duplicate_count']}")

    # Push metrics to XCom
    context['ti'].xcom_push(key='transform_metrics', value=metrics)

    return {
        'clean_path': CLEAN_OUTPUT_PATH,
        'rejected_path': REJECTED_OUTPUT_PATH,
        'metrics': metrics
    }


def load_task(**context):
    """Load clean data into database."""
    print("Starting load task")
    import pandas as pd

    # Load clean data from file
    df_clean = pd.read_csv(CLEAN_OUTPUT_PATH)

    # Convert date column
    df_clean['order_date'] = pd.to_datetime(df_clean['order_date'])

    # Load into database
    load_metrics = load_data_from_file(df_clean, DB_PATH, SCHEMA_PATH)

    print(f"Load completed. Inserted: {load_metrics['inserted']}, "
          f"Updated: {load_metrics['updated']}, Errors: {load_metrics['errors']}")

    # Push load metrics to XCom
    context['ti'].xcom_push(key='load_metrics', value=load_metrics)

    return load_metrics


def validate_task(**context):
    """Validate pipeline results."""
    print("Starting validation task")
    from sqlalchemy import create_engine, text

    # Connect to database
    engine = create_engine(f'sqlite:///{DB_PATH}')

    # Get metrics from previous tasks
    transform_metrics = context['ti'].xcom_pull(task_ids='transform_task', key='transform_metrics')
    load_metrics = context['ti'].xcom_pull(task_ids='load_task', key='load_metrics')

    # Verify record counts
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM orders"))
        db_count = result.scalar()

    print(f"Validation results:")
    print(f"  - Transform accepted: {transform_metrics['accepted_count']}")
    print(f"  - Database records: {db_count}")
    print(f"  - Load inserted: {load_metrics['inserted']}")
    print(f"  - Load updated: {load_metrics['updated']}")

    # Basic validation
    assert db_count > 0, "No records in database"
    assert db_count == transform_metrics['accepted_count'], "Record count mismatch"

    print("Validation passed")


# Create the DAG
with DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='End-to-end ETL pipeline for retail sales data',
    schedule_interval='@daily',  # Run daily
    catchup=False,
    tags=['etl', 'sales', 'retail'],
) as dag:

    extract = PythonOperator(
        task_id='extract_task',
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id='transform_task',
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id='load_task',
        python_callable=load_task,
    )

    validate = PythonOperator(
        task_id='validate_task',
        python_callable=validate_task,
    )

    # Define task dependencies
    extract >> transform >> load >> validate
