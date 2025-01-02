from airflow import DAG
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
import pandas as pd
import pendulum

redshift_conn_id = 'redshift_conn'


def call_stored_procedure(**kwargs):
    from dateutil import relativedelta

    start_datetime = (kwargs.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta.relativedelta(months=1)).strftime("%Y-%m-01")
    end_datetime = (kwargs.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta.relativedelta(months=1)).strftime("%Y-%m-02")
    load_datetime = (kwargs.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta.relativedelta(months=12)).strftime("%Y-%m-%d")

    hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)

    # SQL 쿼리 정의
    querys = ["SET TIME ZONE 'Asia/Seoul';",
              "call stoa_on.sp_d_dhms_upload_loop ('{}', '{}', '{}');".format(start_datetime, end_datetime, load_datetime)]
    
    for query in querys:
        hook.run(query)

# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="dags_python_with_redshiftsqlhook_sp_d_dhms_upload_loop",
    default_args=default_args,
    description="Redshift에서 데이터를 SQLAlchemy 엔진으로 가져오는 DAG",
    schedule='0 0 1 12 *',
    start_date=pendulum.datetime(2024, 11, 18, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    call_stored_procedure = PythonOperator(
        task_id="call_stored_procedure",
        python_callable=call_stored_procedure,
    )

    call_stored_procedure