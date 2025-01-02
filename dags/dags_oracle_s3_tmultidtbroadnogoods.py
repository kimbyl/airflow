from operators.oracle_to_s3_operator import OracleToS3Operator
from airflow import DAG
from airflow import Dataset
import pendulum

dataset_dags_oracle_s3_tmultidtbroadnogoods = Dataset("dags_oracle_s3_tmultidtbroadnogoods")

with DAG(
    dag_id='oracle_to_s3_tmultidtbroadnogoods',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 15, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    tmultidtbroadnogoods_task = OracleToS3Operator(
        task_id="tmultidtbroadnogoods_task",
        outlets=[dataset_dags_oracle_s3_tmultidtbroadnogoods],
        oracle_conn_id='oracle_conn',
        sql= """select 
                    SEQ_FRAME_NO
                    , SEQ_NO
                    , BD_DATE
                    , MEDIA_CODE
                    , BD_BTIME
                    , BTIME
                    , ETIME
                    , GOODS_CODE
                    , TAPE_CODE
                    , CHANNEL_CODE
                    , CHANGE_RATE
                    , SEQ_TESTTAPE_NO
                    , INSERT_ID
                    , INSERT_DATE
                    , MODIFY_ID
                    , MODIFY_DATE
                    from bmtcom.TMULTIDTBROADNOGOODS
                where bd_date >= to_date('{{ (data_interval_end.in_timezone('Asia/Seoul') + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }} 00:00:00', 'yyyy-mm-dd hh24:mi:ss')
                and bd_date < to_date('{{ (data_interval_end.in_timezone('Asia/Seoul') + macros.dateutil.relativedelta.relativedelta(days=2))| ds }} 00:00:00', 'yyyy-mm-dd hh24:mi:ss')""",
        file_path="/tmp/tmultidtbroadnogoods_{{ data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv",
        aws_conn_id='aws_kimbyl_conn',
        s3_bucket='kimbyl-rawdata-sk-stoa',
        s3_key="data/tmultidtbroadnogoods_{{ data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv.gz"
    )    