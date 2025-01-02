from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
import pendulum

def insert_table():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_queries = [ """delete from bmtcom.vpgmmain
                       where seq_frame_no in (select distinct seq_frame_no from bmtcom_wk.vpgmmain_wk);""",
                    """insert into bmtcom.vpgmmain (
                            seq_frame_no,
                            bd_date,
                            channel_code,
                            media_code,
                            prog_code,
                            run_time,
                            tape_name,
                            tape_no,
                            bd_btime,
                            bd_etime,
                            seq_testtape_no,
                            callmulti,
                            seq_no,
                            main_pd,
                            sub_pd,
                            md_code,
                            showhost1,
                            showhost2,
                            showhost3,
                            tape_code,
                            goods_code,
                            sale_price,
                            dc_price,
                            brand_code,
                            lmsd_code,
                            main_yn,
                            analysis_contract_amt,
                            infimum_amt,
                            supremum_amt,
                            weights_time,
                            goods_rate,
                            real_plan_sale_amt,
                            real_plan_margin_amt,
                            pgm_plan_order_amt,
                            pgm_plan_margin_amt,
                            change_rate,
                            auto_plan_sale_amt,
                            auto_plan_margin_amt,
                            modify_date,
                            md_kind
                        )
                        select
                            seq_frame_no,
                            bd_date,
                            channel_code,
                            media_code,
                            prog_code,
                            run_time,
                            tape_name,
                            tape_no,
                            bd_btime,
                            bd_etime,
                            seq_testtape_no,
                            callmulti,
                            seq_no,
                            main_pd,
                            sub_pd,
                            md_code,
                            showhost1,
                            showhost2,
                            showhost3,
                            tape_code,
                            goods_code,
                            sale_price,
                            dc_price,
                            brand_code,
                            lmsd_code,
                            main_yn,
                            analysis_contract_amt,
                            infimum_amt,
                            supremum_amt,
                            weights_time,
                            goods_rate,
                            real_plan_sale_amt,
                            real_plan_margin_amt,
                            pgm_plan_order_amt,
                            pgm_plan_margin_amt,
                            change_rate,
                            auto_plan_sale_amt,
                            auto_plan_margin_amt,
                            modify_date,
                            md_kind
                        from bmtcom_wk.vpgmmain_wk;"""
                ]
    for sql in sql_queries:
        redshift_hook.run(sql)


def select_count():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_query = "select count(*) from bmtcom_wk.vpgmmain_wk;"

    result = redshift_hook.get_records(sql_query)
    count = result[0][0] if result else 0
    return count

def truncate_table():
    redshift_hook = RedshiftSQLHook(
        redshift_conn_id='redshift_conn'
    )

    sql_query = "truncate table bmtcom_wk.vpgmmain_wk;"
    redshift_hook.run(sql_query)
    
# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="dags_python_with_s3_to_redshift_bmtcom_vpgmmain",
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
    
    copy_data_from_s3_to_redshift_bmtcom_vpgmmain = S3ToRedshiftOperator(
        task_id="copy_data_from_s3_to_redshift_bmtcom_vpgmmain",
        schema='dev',
        table='bmtcom_wk.vpgmmain_wk',
        s3_bucket='kimbyl-rawdata-sk-stoa',
        s3_key='data/bmtcom_vpgmmain/vpgmmain_000.gz',
        copy_options=["GZIP", "IGNOREHEADER 1", "DELIMITER ','", "REMOVEQUOTES"],
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

    
    truncate_task >> copy_data_from_s3_to_redshift_bmtcom_vpgmmain >> count_task >> insert_task