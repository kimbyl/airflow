from airflow import DAG
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
import pendulum

with DAG(
    dag_id="dags_redshift_to_s3_dhm2",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2024, 12, 3, tz="Asia/Seoul"),
    catchup=False
) as dag:
    sql= """select
                dhms,
                date,
                hms,
                hh,
                mi,
                sec,
                load_dt
            from STOA_ON.D_DHM"""
    
    transfer_redshift_to_s3 = RedshiftToS3Operator(
        task_id="transfer_redshift_to_s3",
        redshift_conn_id="vision_redshift_conn",
        select_query=sql,
        aws_conn_id="aws_kimbyl_conn",
        s3_bucket="kimbyl-rawdata-sk-stoa",
        s3_key="data/dhm_",     # s3://kimbyl-rawdata-sk-stoa/data/dhm_000.gz
        unload_options=["GZIP", "DELIMITER ','", "ALLOWOVERWRITE", "PARALLEL OFF", "HEADER"]
    )
