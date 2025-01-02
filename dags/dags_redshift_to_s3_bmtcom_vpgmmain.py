from airflow import DAG
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
import pendulum

with DAG(
    dag_id="dags_redshift_to_s3_bmtcom_vpgmmain",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2024, 12, 3, tz="Asia/Seoul"),
    catchup=False
) as dag:

    transfer_redshift_to_s3 = RedshiftToS3Operator(
        task_id="transfer_redshift_to_s3",
        redshift_conn_id="vision_redshift_conn",
        schema="bmtcom",
        table="vpgmmain",
        aws_conn_id="aws_kimbyl_conn",
        s3_bucket="kimbyl-rawdata-sk-stoa",
        s3_key="data/bmtcom_vpgmmain",     # s3://kimbyl-rawdata-sk-stoa/data/bmtcom_vpgmmain/vpgmmain_000.gz
        unload_options=["GZIP", "DELIMITER ','", "ALLOWOVERWRITE", "PARALLEL OFF", "HEADER", "ADDQUOTES"]
    )
