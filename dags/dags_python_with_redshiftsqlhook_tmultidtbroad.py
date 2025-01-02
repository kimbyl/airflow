from airflow import DAG
from airflow import Dataset
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
import pendulum

dataset_dags_oracle_to_s3_tmultidtbroad = Dataset("dags_oracle_to_s3_tmultidtbroad")
redshift_conn_id = 'redshift_conn'

def call_stored_procedure(s3url, s3_key, **kwargs):
    from dateutil import relativedelta
# call bmtcom.sp_tmultidtbroad_upload ('dep_sub_date('YYYY-MM-DD 00:00:00', 1, days)', 'dep_add_date('YYYY-MM-DD 00:00:00', 2, days)', '#{dep_s3_url}' );
    start_datetime = (kwargs.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta.relativedelta(minutes=-30)).strftime("%Y-%m-%d %H:%M:00")
    end_datetime = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime("%Y-%m-%d %H:%M:00")

    hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)

    # SQL 쿼리 정의
    querys = ["SET TIME ZONE 'Asia/Seoul';",
              "call bmtcom.sp_tmultidtbroad_upload ('{}', '{}', 's3://{}/{}');".format(start_datetime, end_datetime, s3url, s3_key)]
    
    for query in querys:
        hook.run(query)

def truncate_tables(**kwargs):
    hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
    # SQL 쿼리 정의
    querys = ["truncate bmtcom_wk.tmultidtbroad_wk;",]
    for query in querys:
        hook.run(query)

# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="dags_python_with_redshiftsqlhook_tmultidtbroad",
    default_args=default_args,
    description="Redshift에서 데이터를 SQLAlchemy 엔진으로 가져오는 DAG",
    schedule=[dataset_dags_oracle_to_s3_tmultidtbroad],
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
        op_kwargs={'s3url': 'kimbyl-rawdata-sk-stoa', 
                   's3_key': 'data/tmultiframesche_{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}.csv.gz'}
    )

    truncate_tables >> call_stored_procedure 