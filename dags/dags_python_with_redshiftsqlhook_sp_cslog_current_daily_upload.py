from airflow import DAG
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
import pandas as pd
import pendulum

redshift_conn_id = 'redshift_conn'

def load_data_from_redshift(**kwargs):
    """RedshiftSQLHook의 get_sqlalchemy_engine()을 사용하여 Redshift 데이터 가져오기"""
    redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)

    # SQLAlchemy 엔진 가져오기
    engine = redshift_hook.get_sqlalchemy_engine()

    # SQL 쿼리 정의
    query = "SELECT * FROM BMTCOM.TORDERPROMO LIMIT 10"

    # Pandas를 사용하여 Redshift에서 데이터 가져오기
    with engine.connect() as connection:
        result = connection.execute(query)
        for row in result:
            print(row)
        # df = pd.read_sql(query, connection)

    # 데이터 출력
    # print(df)

def call_stored_procedure(**kwargs):
    from dateutil import relativedelta

    start_datetime = (kwargs.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta.relativedelta(minutes=-30)).strftime("%Y-%m-%d %H:%M:00")
    end_datetime = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime("%Y-%m-%d %H:%M:00")

    hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)

    # SQL 쿼리 정의
    querys = ["SET TIME ZONE 'Asia/Seoul';",
              "call stoa_log.sp_cslog_current_daily_upload ('{}', '{}');".format(start_datetime, end_datetime)]
    
    for query in querys:
        hook.run(query)

def truncate_tables(**kwargs):
    hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
    # SQL 쿼리 정의
    querys = ["truncate table stoa_log.cslog_current_work_01;", 
              "truncate table stoa_log.cslog_current_daily;"]
    for query in querys:
        hook.run(query)

# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="dags_python_with_redshiftsqlhook_sp_cslog_current_daily_upload",
    default_args=default_args,
    description="Redshift에서 데이터를 SQLAlchemy 엔진으로 가져오는 DAG",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 18, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    truncate_tables = PythonOperator(
        task_id="truncate_tables",
        python_callable=truncate_tables,
    )

    call_stored_procedure = PythonOperator(
        task_id="call_stored_procedure",
        python_callable=call_stored_procedure,
    )

    truncate_tables >> call_stored_procedure