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
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

project_id = Variable.get("project_id")
region = Variable.get("region")
bucket_name = Variable.get("bucket_name")



with models.DAG(
    "dagtest1",
   default_args = {
    "project_id": project_id,
    "region": region,
    "bucket_name": bucket_name,
    "owner": 'user',
    "depends_on_past": True,
    "email": ['gsikka22@gmail.com'],
    "email_on_failure": True,
    "email_on_retry": False,
    "start_date": datetime(2024, 3, 27),
    "schedule_interval": '@daily'
},
    description="This is a basic task",
    tags=["COE"]
) as dag:

    task1 = DummyOperator(
        task_id="start_OE1"
    )

    task2 = BashOperator(
        task_id="OE_BASH_task",
        bash_command="echo 'Bash command executed successfully'"
    )

    task3 = DummyOperator(
        task_id="end_oe_task"
    )

    task1 >> task2 >> task3
