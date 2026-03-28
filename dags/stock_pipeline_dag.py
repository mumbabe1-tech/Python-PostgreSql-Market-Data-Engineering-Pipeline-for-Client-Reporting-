from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Import  pipeline function (this runs ETL)
from stock_pipeline import run_pipeline

# Import  validation function (this checks the loaded data)
from stock_pipeline_validation import validate_stock_data


# The DAG definition block
with DAG(
    dag_id="stock_price_pipeline",      # This is the name that appears in the Airflow UI
    start_date=datetime(2026, 3, 21),   # Airflow will not run anything before this date
    schedule="@daily",                  # Runs once per day
    catchup=False,                      # Prevents Airflow from running past dates
    tags=["stockprices", "Apex"],       # Helps organize DAGs in the UI
) as dag:                             
    
    # Task 1: Run pipeline
    t1 = PythonOperator(
        task_id="run_pipeline",         # Task name shown in the UI
        python_callable=run_pipeline,   # This calls your ETL function
    )
    
    # Task 2: Validate the loaded data
    validate_task = PythonOperator(
        task_id="validate_loaded_data",     # Task name shown in the UI
        python_callable=validate_stock_data # Calls your validation function
    )

    # Task dependency: run pipeline first, then validate
    t1 >> validate_task