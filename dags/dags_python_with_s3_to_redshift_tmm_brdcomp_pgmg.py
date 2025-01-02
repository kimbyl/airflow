from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
import pendulum

def insert_table():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_queries = [ """delete from dap.tmm_brdcomp_pgmg_mst 
                        where brd_dt in 
                             (select distinct brd_dt from dap.tmm_brdcomp_pgmg_work);""",
                    """insert into dap.tmm_brdcomp_pgmg_mst (
                            brdcomp_cd
                            , ctgr_nm
                            , brd_dt
                            , brd_stt_tslot_cd
                            , brd_stt_hmi
                            , brd_end_hmi
                            , brd_titl
                            , sale_prc
                            , brdcomp_hngl_nm
                            , o01_brdcomp_gb_nm
                            , o02_brdcomp_gb_nm
                            , instr_id
                            , inst_dttm
                            , mdfr_id
                            , mdf_dttm
                            , etl_load_dttm
                            , dc_aft_prc
                            , cnsl_rsrv_gds_yn
                            , main_yn)
                        SELECT brdcomp_cd
                            , ctgr_nm
                            , brd_dt
                            , brd_stt_tslot_cd
                            , brd_stt_hmi
                            , brd_end_hmi
                            , brd_titl
                            , sale_prc
                            , brdcomp_hngl_nm
                            , o01_brdcomp_gb_nm
                            , o02_brdcomp_gb_nm
                            , instr_id
                            , inst_dttm
                            , mdfr_id
                            , mdf_dttm
                            , etl_load_dttm
                            , dc_aft_prc
                            , cnsl_rsrv_gds_yn
                            , main_yn
                        FROM dap.tmm_brdcomp_pgmg_work
                        where brd_stt_tslot_cd <> '99';"""
                ]
    for sql in sql_queries:
        redshift_hook.run(sql)


def select_count():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_query = "select count(*) from dap.tmm_brdcomp_pgmg_work;"

    result = redshift_hook.get_records(sql_query)
    count = result[0][0] if result else 0
    return count

def truncate_table():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_query = "truncate table dap.tmm_brdcomp_pgmg_work;"
    redshift_hook.run(sql_query)
    
# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="dags_python_s3_to_redshift_tmm_brdcomp_pgmg",
    default_args=default_args,
    description="Redshift에서 데이터를 SQLAlchemy 엔진으로 가져오는 DAG",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 18, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    truncate_task = PythonOperator(
        task_id='truncate_table_task',
        python_callable=truncate_table
    )
    
    copy_data_from_s3_to_redshift_dap_tmm_brdcomp_pgmg_work = S3ToRedshiftOperator(
        task_id="copy_data_from_s3_to_redshift_dap_tmm_brdcomp_pgmg_work",
        schema='dev',
        table='dap.tmm_brdcomp_pgmg_work',
        s3_bucket='skstoa-prd-datalake-stoaon-upload',
        s3_key='HSMOA/{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds_nodash }}/',
        copy_options=['FORMAT AS PARQUET'],
        aws_conn_id='s3_stoaon_id',
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

    
    truncate_task >> copy_data_from_s3_to_redshift_dap_tmm_brdcomp_pgmg_work >> count_task >> insert_task