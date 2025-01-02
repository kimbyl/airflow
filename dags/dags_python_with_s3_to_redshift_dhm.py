from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
import pendulum

def insert_table():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_queries = [ """delete from STOA_ON.D_DHM
                       where dhms in (select distinct dhms from STOA_ON.D_DHM_WK);""",
                    """insert into STOA_ON.D_DHM (
                            dhms,
                            date,
                            hms,
                            hh,
                            mi,
                            sec,
                            load_dt
                        )
                        select
                            dhms,
                            date,
                            hms,
                            hh,
                            mi,
                            sec,
                            load_dt
                        from STOA_ON.D_DHM_WK;""",

                    """delete from STOA_LOG.D_DHM
                       where dhms in (select distinct dhms from STOA_ON.D_DHM_WK);""",
                    """insert into STOA_LOG.D_DHM (
                            dhms,
                            date,
                            hms,
                            hh,
                            mi,
                            sec,
                            load_dt
                        )
                        select
                            dhms,
                            date,
                            hms,
                            hh,
                            mi,
                            sec,
                            load_dt
                        from STOA_ON.D_DHM_WK;""",
                ]
    for sql in sql_queries:
        redshift_hook.run(sql)


def select_count():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_query = "select count(*) from STOA_ON.D_DHM_WK;"

    result = redshift_hook.get_records(sql_query)
    count = result[0][0] if result else 0
    return count

def truncate_table():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_query = "truncate table STOA_ON.D_DHM_WK;"
    redshift_hook.run(sql_query)
    
# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="dags_python_with_s3_to_redshift_dhm",
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
    
    copy_data_from_s3_to_redshift_d_dhm = S3ToRedshiftOperator(
        task_id="copy_data_from_s3_to_redshift_d_dhm",
        schema='dev',
        table='STOA_ON.D_DHM_WK',
        s3_bucket='kimbyl-rawdata-sk-stoa',
        s3_key='data/dhm_000.gz',
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

    
    truncate_task >> copy_data_from_s3_to_redshift_d_dhm >> count_task >> insert_task