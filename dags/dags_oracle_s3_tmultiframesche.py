from operators.oracle_to_s3_operator import OracleToS3Operator
from airflow import DAG
from airflow import Dataset
import pendulum

dataset_dags_oracle_s3_tmultiframesche = Dataset("dags_oracle_s3_tmultiframesche")

with DAG(
    dag_id='oracle_to_s3_tmultiframesche',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 15, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    tmultiframesche_task = OracleToS3Operator(
        task_id="tmultiframesche_task",
        outlets=[dataset_dags_oracle_s3_tmultiframesche],
        oracle_conn_id='oracle_conn',
        sql= """select
                    seq_frame_no
                    , bd_date
                    , media_code
                    , prog_code
                    , bd_btime
                    , bd_etime
                    , main_pd
                    , sub_pd
                    , pc_userid
                    , camera_team
                    , sub_team
                    , live_flag
                    , tape_code
                    , plan_amt
                    , plan_order_amt
                    , plan_margin_amt
                    , weights_time
                    , main_pd_rate
                    , sub_pd_rate
                    , model
                    , guest
                    , broad_fixed_amt_yn
                    , broad_fixed_amt
                    , subtitle_yn
                    , insert_date
                    , modify_date
                    , analysis_contract_amt
                    , broad_display_yn
                    , channel_code
                    , seq_testtape_no
                    , callmulti
                    , infimum_amt
                    , supremum_amt
                    , etc
                    , sub_pd2
                -- , sysdate as load_dt
                from bmtcom.tmultiframesche
                where bd_date >= to_date('{{ (data_interval_end.in_timezone('Asia/Seoul') + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }} 00:00:00', 'yyyy-mm-dd hh24:mi:ss')
                and bd_date < to_date('{{ (data_interval_end.in_timezone('Asia/Seoul') + macros.dateutil.relativedelta.relativedelta(days=2))| ds }} 00:00:00', 'yyyy-mm-dd hh24:mi:ss')""",
        file_path="/tmp/tmultiframesche_{{ data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv",
        aws_conn_id='aws_kimbyl_conn',
        s3_bucket='kimbyl-rawdata-sk-stoa',
        s3_key="data/tmultiframesche_{{ data_interval_end.in_timezone('Asia/Seoul') | ds_nodash }}.csv.gz"
    )    