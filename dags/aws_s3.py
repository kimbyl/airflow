from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_conn')
    hook.load_file(filename=filename,
                   key=key,
                   bucket_name=bucket_name,
                   replace=True)


with DAG('upload_to_s3',
         schedule_interval=None,
         start_date=datetime(2022, 1, 1),
         catchup=False
         ) as dag:
    upload = PythonOperator(task_id='upload',
                            python_callable=upload_to_s3,
                            op_kwargs={
                                'filename': '/opt/airflow/data/20241001A.csv',
                                'key': '20241001A.csv',
                                'bucket_name': 'skstoadev-nielsen'
                            })
    upload