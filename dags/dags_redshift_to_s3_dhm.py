from airflow import DAG
from operators.redshift_to_s3_operator import RedshiftToS3Operator
import pendulum
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    "dags_redshift_to_s3_dhm",
    default_args=default_args,
    description="Redshift 데이터를 S3에 업로드하는 DAG with RedshiftSQLHook",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 11, 28, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    # FAILED: 
    # heartbeat timeout		
    # Task did not emit heartbeat within time limit (600 seconds) and will be terminated. See https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#zombie-undead-tasks
    dhm_task = RedshiftToS3Operator(
        task_id="dhm_task",
        # outlets=[dataset_dags_oracle_s3_dhm],
        redshift_conn_id='vision_redshift_conn',
        sql= """select
                dhms,
                date,
                hms,
                hh,
                mi,
                sec,
                load_dt
            from STOA_ON.D_DHM""",
        file_path="/tmp/dhm_{{ data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv",
        aws_conn_id='aws_kimbyl_conn',
        s3_bucket='kimbyl-rawdata-sk-stoa',
        s3_key="data/dhm_{{ data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv.gz",
        execution_timeout=timedelta(minutes=5)
    )    

    dhm_task