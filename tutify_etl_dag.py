from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import os
import pandas as pd

# === CONFIGURE PATHS ===
RAW_DIR = '/home/uthred/airflow/data/raw'
CLEAN_DIR = '/home/uthred/airflow/data/clean'
os.makedirs(CLEAN_DIR, exist_ok=True)

# === CLEANING FUNCTIONS ===
def clean_student_payment():
    path = os.path.join(RAW_DIR, 'FACT_STUDENT_PAYMENT.csv')
    df = pd.read_csv(path)
    numeric_cols = ['payment_id', 'student_id', 'subject_id', 'date_id', 
                    'amount_original', 'exchange_rate', 'amount_converted_pkr', 
                    'payment_method_id']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    out_path = os.path.join(CLEAN_DIR, 'FACT_STUDENT_PAYMENT_CLEAN.csv')
    df.to_csv(out_path, index=False)

def clean_teacher_payout():
    path = os.path.join(RAW_DIR, 'FACT_TEACHER_PAYOUT.csv')
    df = pd.read_csv(path)
    numeric_cols = ['payout_id', 'teacher_id', 'date_id', 'hours_taught', 
                    'rate_per_hour', 'total_payout', 'bonus']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    out_path = os.path.join(CLEAN_DIR, 'FACT_TEACHER_PAYOUT_CLEAN.csv')
    df.to_csv(out_path, index=False)

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'snowflake_conn_id': 'my_snowflake_conn',
    'catchup': False,
}

with DAG(
    dag_id='tutify_etl_dag',
    default_args=default_args,
    schedule=None,
    tags=['tutify', 'etl', 'snowflake'],
    catchup=False
) as dag:

    # Cleaning tasks
    clean_student_task = PythonOperator(
        task_id='clean_student_payment',
        python_callable=clean_student_payment
    )

    clean_teacher_task = PythonOperator(
        task_id='clean_teacher_payout',
        python_callable=clean_teacher_payout
    )

    # Upload cleaned files to Snowflake stage
    upload_student_task = SnowflakeOperator(
        task_id='upload_student_to_stage',
        sql=f"""
            USE WAREHOUSE TUTIFY_WH;
            USE DATABASE TUTIFY_DB;
            USE SCHEMA TUTIFY_SCHEMA;
            PUT file://{CLEAN_DIR}/FACT_STUDENT_PAYMENT_CLEAN.csv @my_stage OVERWRITE=TRUE;
        """,
        snowflake_conn_id='my_snowflake_conn',
    )

    upload_teacher_task = SnowflakeOperator(
        task_id='upload_teacher_to_stage',
        sql=f"""
            USE WAREHOUSE TUTIFY_WH;
            USE DATABASE TUTIFY_DB;
            USE SCHEMA TUTIFY_SCHEMA;
            PUT file://{CLEAN_DIR}/FACT_TEACHER_PAYOUT_CLEAN.csv @my_stage OVERWRITE=TRUE;
        """,
        snowflake_conn_id='my_snowflake_conn',
    )

    # Load staging tables from stage
    load_stg_student_task = SnowflakeOperator(
        task_id='load_stg_student_payment',
        sql="""
            USE WAREHOUSE TUTIFY_WH;
            USE DATABASE TUTIFY_DB;
            USE SCHEMA TUTIFY_SCHEMA;

            COPY INTO STG_STUDENT_PAYMENT
            FROM @my_stage/FACT_STUDENT_PAYMENT_CLEAN.csv
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
        """,
        snowflake_conn_id='my_snowflake_conn',
    )

    load_stg_teacher_task = SnowflakeOperator(
        task_id='load_stg_teacher_payout',
        sql="""
            USE WAREHOUSE TUTIFY_WH;
            USE DATABASE TUTIFY_DB;
            USE SCHEMA TUTIFY_SCHEMA;

            COPY INTO STG_TEACHER_PAYOUT
            FROM @my_stage/FACT_TEACHER_PAYOUT_CLEAN.csv
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
        """,
        snowflake_conn_id='my_snowflake_conn',
    )

    # Create or replace fact tables from staging data
    create_fact_student_task = SnowflakeOperator(
        task_id='create_fact_student_payment',
        sql="""
            USE WAREHOUSE TUTIFY_WH;
            USE DATABASE TUTIFY_DB;
            USE SCHEMA TUTIFY_SCHEMA;

            CREATE OR REPLACE TABLE FACT_STUDENT_PAYMENT AS
            SELECT * FROM STG_STUDENT_PAYMENT;
        """,
        snowflake_conn_id='my_snowflake_conn',
    )

    create_fact_teacher_task = SnowflakeOperator(
        task_id='create_fact_teacher_payout',
        sql="""
            USE WAREHOUSE TUTIFY_WH;
            USE DATABASE TUTIFY_DB;
            USE SCHEMA TUTIFY_SCHEMA;

            CREATE OR REPLACE TABLE FACT_TEACHER_PAYOUT AS
            SELECT * FROM STG_TEACHER_PAYOUT;
        """,
        snowflake_conn_id='my_snowflake_conn',
    )

    # Task dependencies - linear per stream (student & teacher)
    clean_student_task >> upload_student_task >> load_stg_student_task >> create_fact_student_task
    clean_teacher_task >> upload_teacher_task >> load_stg_teacher_task >> create_fact_teacher_task
