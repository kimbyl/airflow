from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import Dataset

dataset_sftp_operators_producer = Dataset("sftp_operators_producer")

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('aws_conn')
    hook.load_file(filename=filename,
                   key=key,
                   bucket_name=bucket_name,
                   replace=True)


with DAG('upload_to_s3',
         schedule=[dataset_sftp_operators_producer],
         start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
         catchup=False
         ) as dag:
    upload = PythonOperator(task_id='upload',
                            python_callable=upload_to_s3,
                            op_kwargs={
                                'filename': '/opt/airflow/data/{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds_nodash }}A.csv',
                                'key': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds_nodash }}A.csv',
                                'bucket_name': 'skstoadev-nielsen'
                            })
    upload