from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import pendulum

def fetch_data_from_oracle(**kwargs):
    from dateutil import relativedelta
    from contextlib import closing
    import csv

    start_date = (kwargs.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta.relativedelta(months=-1)).strftime("%Y-%m-01")
    end_date = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y-%m-%d')

    oracle_hook = OracleHook(oracle_conn_id="oracle_conn")

    sql = """select
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
            where INSERT_DATE >= to_date('{} 00:00:00', 'yyyy-mm-dd hh24:mi:ss')
            and INSERT_DATE < to_date('{} 00:00:00', 'yyyy-mm-dd hh24:mi:ss')""".format(start_date, end_date)

    file_path = f"/tmp/torderpromo_{end_date}.csv"

    # with closing(oracle_hook.get_conn()) as conn:
    #     with closing(conn.cursor()) as cursor:
    #         cursor.execute(sql)
    #         with open(file_path, 'w', encoding='utf-8') as fp:
    #             wr = csv.writer(fp)
    #             for row in cursor:
    #                 wr.writerow(row)                    

    df = oracle_hook.get_pandas_df(sql)
    df.to_csv(file_path, index=False, encoding='utf-8')     # header=False 헤더 제거

    return file_path

def upload_to_s3(file_path: str, **kwargs) -> None:
    end_date = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y%m%d')
    hook = S3Hook(aws_conn_id='aws_kimbyl_conn')
    hook.load_file(filename=file_path,
                   key=f'data/torderpromo_{end_date}.csv.gz',
                   bucket_name='kimbyl-rawdata-sk-stoa',
                   gzip=True,
                   replace=True)
    os.remove(file_path)

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    "dags_python_with_oracle_to_s3_operator",
    default_args=default_args,
    description="Oracle 데이터를 S3에 업로드하는 DAG with OracleHook",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 14, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_data_from_oracle",
        python_callable=fetch_data_from_oracle,
    )

    upload_data = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_data_from_oracle') }}"],
    )

    fetch_data >> upload_data