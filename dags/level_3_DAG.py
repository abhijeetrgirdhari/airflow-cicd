# Import required modules
import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from datetime import timedelta
import logging

# variable section
PROJECT_ID = "sandbox-explorer-490214"
REGION = "us-central1"
GCS_BUCKET = "test-bucket-2015"

# Using a public Dataflow template (instead of Python file)
TEMPLATE_PATH = "gs://dataflow-templates/latest/Word_Count"

ARGS = {
    "owner": "kanchan",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": True,
    "email_on_retry": True,
    "email": ["abhijeetrgirdhari@gmail.com"],
    "execution_timeout": timedelta(minutes=30),
}

# Define the DAG
with DAG(
    dag_id="LEVEL_5_DAG",
    default_args=ARGS,
    schedule_interval="0 6 * * *",  # Runs daily at 6 AM
    description="DAG to run Dataflow Template",
    catchup=False,
    max_active_runs=1,
    tags=["dataflow", "etl", "data_engineering"],
) as dag:

    # Task: Run Dataflow Template
    submit_beam_job = DataflowTemplatedJobStartOperator(
        task_id="run_dataflow_template",
        template=TEMPLATE_PATH,
        job_name="my-dataflow-job",
        parameters={
            "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
            "output": f"gs://{GCS_BUCKET}/output/result",
        },
        location=REGION,
        gcp_conn_id="google_cloud_default",
    )
