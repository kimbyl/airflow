from airflow import DAG
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
import pandas as pd
import pendulum

redshift_conn_id = 'redshift_conn'

def load_data_from_redshift(**kwargs):
    """RedshiftSQLHook의 get_sqlalchemy_engine()을 사용하여 Redshift 데이터 가져오기"""
    hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)

    # SQL 쿼리 정의
    query = "SELECT count(*) FROM dap.tmm_brdcomp_pgmg_work;"

    result = hook.get_records(query)
    count = result[0][0] if result else 0
    print(count)

# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="dags_python_with_redshiftsqlhook_dap_tmm_brdcomp_pgmg_work",
    default_args=default_args,
    description="Redshift에서 데이터를 SQLAlchemy 엔진으로 가져오는 DAG",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 18, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    sql_query = PythonOperator(
        task_id="load_data_from_redshift_dap_tmm_brdcomp_pgmg_work",
        python_callable=load_data_from_redshift,
    )
    
    sql_query