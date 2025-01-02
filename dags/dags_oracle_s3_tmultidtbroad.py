from operators.oracle_to_s3_operator import OracleToS3Operator
from airflow import DAG
from airflow import Dataset
import pendulum

dataset_dags_oracle_to_s3_tmultidtbroad = Dataset("dags_oracle_to_s3_tmultidtbroad")

with DAG(
    dag_id='oracle_to_s3_tmultidtbroad',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 15, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    tmultidtbroad_task = OracleToS3Operator(
        task_id="tmultidtbroad_task",
        outlets=[dataset_dags_oracle_to_s3_tmultidtbroad],
        oracle_conn_id='oracle_conn',
        sql= """select 
                    SEQ_FRAME_NO
                    , SEQ_NO
                    , BD_DATE
                    , MEDIA_CODE
                    , BD_BTIME
                    , PROG_CODE
                    , BTIME
                    , ETIME
                    , RUN_TIME
                    , SALE_END_TIME
                    , GOODS_CODE
                    , BD_FLAG
                    , MAIN_YN
                    , BROAD_YN
                    , PDIN_YN
                    , SALE_TG_QTY
                    -- , SPEC_NOTE
                    , INSERT_DATE
                    , MODIFY_DATE
                    , WEIGHTS_TIME
                    , GOODS_RATE
                    , REAL_PLAN_SALE_AMT
                    , REAL_PLAN_MARGIN_AMT
                    , PGM_PLAN_ORDER_AMT
                    , PGM_PLAN_MARGIN_AMT
                    , CHANGE_RATE
                    , CHANNEL_CODE
                    , AUTO_PLAN_SALE_AMT
                    from bmtcom.TMULTIDTBROAD
                where bd_date >= to_date('{{ (data_interval_end.in_timezone('Asia/Seoul') + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }} 00:00:00', 'yyyy-mm-dd hh24:mi:ss')
                and bd_date < to_date('{{ (data_interval_end.in_timezone('Asia/Seoul') + macros.dateutil.relativedelta.relativedelta(days=2))| ds }} 00:00:00', 'yyyy-mm-dd hh24:mi:ss')""",
        file_path="/tmp/tmultidtbroad_{{ data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv",
        aws_conn_id='aws_kimbyl_conn',
        s3_bucket='kimbyl-rawdata-sk-stoa',
        s3_key="data/tmultidtbroad_{{ data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv.gz"
    )    