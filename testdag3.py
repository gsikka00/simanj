from airflow import models
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator
)
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

project_id = Variable.get("project_id")
region = Variable.get("region")
bucket_name = Variable.get("bucket_name")

def decide_branch(**kwargs):
    if kwargs['execution_date'].day % 2 == 0:
        return 'even_branch'
    else:
        return 'odd_branch'

with models.DAG(
    "testdag3",
    default_args={
        "project_id": project_id,
        "region": region,
        "bucket_name": bucket_name,
        "owner": 'user',
        "email": ["gsikka22@gmail.com"],
        "email_on_success": True,
        "email_on_failure": True,
        "email_on_retry": False,
        "start_date": datetime(2024, 3, 27),
        "schedule_interval": '@daily'
    },
    description="this is test dag3",
    tags=["COE"]
) as dag:

    start_task = DummyOperator(
        task_id="Starting_COE",
        dag=dag
    )

    branch_task = PythonOperator(
        task_id="branching",
        python_callable=decide_branch,
        provide_context=True,
        dag=dag
    )

    end_task = DummyOperator(
        task_id="end_COE",
        dag=dag
    )

    start_task >> branch_task >> end_task
