from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import pendulum

def fetch_data(**kwargs):
    from dateutil import relativedelta
    from contextlib import closing
    import csv

    hook = RedshiftSQLHook(redshift_conn_id="vision_redshift_conn")

    sql = """select
                mi,
                load_dt
            from STOA_ON.D_MINUTE"""

    file_path = f"/tmp/d_minute.csv"

    df = hook.get_pandas_df(sql)
    df.to_csv(file_path, index=False, encoding='utf-8')     # header=False 헤더 제거

    return file_path

def upload_to_s3(file_path: str, **kwargs) -> None:
    end_date = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y%m%d')
    hook = S3Hook(aws_conn_id='aws_kimbyl_conn')
    hook.load_file(filename=file_path,
                   key=f'data/d_minute.csv.gz',
                   bucket_name='kimbyl-rawdata-sk-stoa',
                   gzip=True,
                   replace=True)
    os.remove(file_path)

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    "dags_python_with_redshift_to_s3_minute",
    default_args=default_args,
    description="Redshift 데이터를 S3에 업로드하는 DAG with RedshiftSQLHook",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 28, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_data_from_redshift",
        python_callable=fetch_data,
    )

    upload_data = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_data_from_redshift') }}"],
    )

    fetch_data >> upload_data