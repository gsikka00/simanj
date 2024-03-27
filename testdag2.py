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
from datetime import datetime, timedelta
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

project_id = Variable.get("project_id")
region = Variable.get("region")
bucket_name = Variable.get("bucket_name")

def print_data():
    return "Hello cloud composer"

with models.DAG(
    'TESTDAG2',
    default_args={
        "project_id": project_id,
        "region": region,
        "bucket_name": bucket_name,
        "owner": 'user',
        "email": ["gsikka22@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "start_date": datetime(2024, 3, 27),
        "schedule_interval": '@daily'
    },
    description="This is a test dag2",
    tags=["COE"]
) as dag:

    task1 = DummyOperator(
        task_id="startingOE2",
        dag=dag
    )

    task2 = BashOperator(
        task_id="Middleoe2",
        bash_command="echo middle",
        dag=dag
    )

    task3 = PythonOperator(
        task_id="running_python",
        python_callable=print_data,
        dag=dag
    )

    task4 = DummyOperator(
        task_id="EndOE2",
        dag=dag
    )

    task1 >> task2 >> task3 >> task4
