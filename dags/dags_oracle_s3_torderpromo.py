from operators.oracle_to_s3_operator import OracleToS3Operator
from airflow import DAG
from airflow import Dataset
import pendulum


dataset_dags_oracle_s3_torderpromo = Dataset("dags_oracle_s3_torderpromo")

with DAG(
    dag_id='oracle_to_s3_torderpromo',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 15, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    torderpromo_task = OracleToS3Operator(
        task_id="torderpromo_task",
        outlets=[dataset_dags_oracle_s3_torderpromo],
        oracle_conn_id='oracle_conn',
        sql= """select
                    SEQ
                    , PROMO_NO
                    , DO_TYPE
                    , ORDER_NO
                    , ORDER_G_SEQ
                    , ORDER_D_SEQ
                    , ORDER_W_SEQ
                    , PROC_AMT
                    , CANCEL_AMT
                    , CALIM_AMT
                    , PROC_COST
                    , OWN_PROC_COST
                    , ENTP_PROC_COST
                    , ENTP_CODE
                    , RECEIPT_NO
                    , CANCEL_YN
                    , CANCEL_DATE
                    , INSERT_DATE
                    , ALLIANCE_PROC_COST
                from BMTCOM.TORDERPROMO
                where INSERT_DATE >= to_date('{{ macros.ds_format( (data_interval_end.in_timezone('Asia/Seoul') - macros.dateutil.relativedelta.relativedelta(months=1)) | ds, '%Y-%m-%d', '%Y-%m-01') }} 00:00:00', 'yyyy-mm-dd hh24:mi:ss')
                and INSERT_DATE < to_date('{{ data_interval_end.in_timezone('Asia/Seoul') | ds }} 00:00:00', 'yyyy-mm-dd hh24:mi:ss')""",
        file_path="/tmp/torderpromo_{{ data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv",
        aws_conn_id='aws_kimbyl_conn',
        s3_bucket='kimbyl-rawdata-sk-stoa',
        s3_key="data/torderpromo_{{ data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv.gz"
    )    