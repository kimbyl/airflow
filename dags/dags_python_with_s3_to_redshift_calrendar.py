from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
import pendulum

def insert_table():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_queries = [ """delete from stoa_on.d_calendar
                       where date in (select distinct date from stoa_on.d_calendar_wk);""",
                    """insert into stoa_on.d_calendar (
                            date_id      ,
                            date         ,
                            sday         ,
                            year         ,
                            month        ,
                            day          ,
                            quarter      ,
                            week_ori     ,
                            week         ,
                            month_week   ,
                            day_name     ,
                            month_name   ,
                            month_id     ,
                            week_ori_id  ,
                            week_id      ,
                            holiday_flag ,
                            weekend_flag ,
                            day_no      )
                        SELECT date_id,
                            date         ,
                            sday         ,
                            year         ,
                            month        ,
                            day          ,
                            quarter      ,
                            week_ori     ,
                            week         ,
                            month_week   ,
                            day_name     ,
                            month_name   ,
                            month_id     ,
                            week_ori_id  ,
                            week_id      ,
                            holiday_flag ,
                            weekend_flag ,
                            day_no      
                        FROM stoa_on.d_calendar_wk;""",
                    """delete from stoa_log.d_calendar
                       where date in (select distinct date from stoa_on.d_calendar_wk);""",
                    """insert into stoa_log.d_calendar (
                            date_id      ,
                            date         ,
                            sday         ,
                            year         ,
                            month        ,
                            day          ,
                            quarter      ,
                            week_ori     ,
                            week         ,
                            month_week   ,
                            day_name     ,
                            month_name   ,
                            month_id     ,
                            week_ori_id  ,
                            week_id      ,
                            holiday_flag ,
                            weekend_flag ,
                            day_no      )
                        SELECT date_id,
                            date         ,
                            sday         ,
                            year         ,
                            month        ,
                            day          ,
                            quarter      ,
                            week_ori     ,
                            week         ,
                            month_week   ,
                            day_name     ,
                            month_name   ,
                            month_id     ,
                            week_ori_id  ,
                            week_id      ,
                            holiday_flag ,
                            weekend_flag ,
                            day_no      
                        FROM stoa_on.d_calendar_wk;"""
                ]
    for sql in sql_queries:
        redshift_hook.run(sql)


def select_count():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_query = "select count(*) from stoa_on.d_calendar_wk;"

    result = redshift_hook.get_records(sql_query)
    count = result[0][0] if result else 0
    return count

def truncate_table():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_query = "truncate table stoa_on.d_calendar_wk;"
    redshift_hook.run(sql_query)
    
# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="dags_python_with_s3_to_redshift_calrendar",
    default_args=default_args,
    description="S3에서 데이터를 Redshift로 가져오는 DAG",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 18, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    truncate_task = PythonOperator(
        task_id='truncate_table_task',
        python_callable=truncate_table
    )
    
    copy_data_from_s3_to_redshift_d_calrendar = S3ToRedshiftOperator(
        task_id="copy_data_from_s3_to_redshift_d_calrendar",
        schema='dev',
        table='stoa_on.d_calendar_wk',
        s3_bucket='kimbyl-rawdata-sk-stoa',
        s3_key='data/d_calendar.csv.gz',
        copy_options=["GZIP", "IGNOREHEADER 1", "DELIMITER ','"],
        aws_conn_id='aws_kimbyl_conn',
        redshift_conn_id='redshift_conn'
    )

    count_task = PythonOperator(
        task_id='count_task',
        python_callable=select_count
    )

    insert_task = PythonOperator(
        task_id="insert_task",
        python_callable=insert_table
    )

    
    truncate_task >> copy_data_from_s3_to_redshift_d_calrendar >> count_task >> insert_task