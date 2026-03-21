from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from stock_pipeline import run_pipeline

with DAG(
    dag_id="stock_price_pipeline",
    start_date=datetime(2026, 3, 21),
    schedule="@daily",
    catchup=False,
    tags=["stockprices", "Apex"],
) as dags:

    t1 = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )