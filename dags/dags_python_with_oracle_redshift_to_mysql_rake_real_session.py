from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import pendulum

def fetch_data_from_oracle(**kwargs):
    from dateutil import relativedelta
    # from contextlib import closing
    # import csv

    start_date = (kwargs.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta.relativedelta(months=-1)).strftime("%Y-%m-01")
    end_date = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
    end_datetime = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y-%m-%d %H:%M:00')

    oracle_hook = OracleHook(oracle_conn_id="oracle_conn")

    sql_PgmTapeCodeList = """SELECT to_char(bd_btime, 'yyyymmddhh24mi'),
                                media_code,
                                channel_code,
                                tape_code
                            FROM bmtcom.tmultiframesche
                            WHERE bd_date = to_date(to_char(sysdate + 9 / 24, 'yyyy-mm-dd'), 'yyyy-mm-dd')
                            or bd_date = to_date(to_char((sysdate + 9 / 24) - 1, 'yyyy-mm-dd'), 'yyyy-mm-dd')"""

    df = oracle_hook.get_pandas_df(sql_PgmTapeCodeList)
    print(df)

    sql_PgmSchedule = """SELECT to_char(bd_btime, 'yyyymmddhh24mi'),
                           to_char(bd_etime, 'yyyymmddhh24mi')
                        FROM bmtcom.vpgmmain
                        WHERE bd_date = to_date(to_char(sysdate + 9 / 24, 'yyyy-mm-dd'), 'yyyy-mm-dd')
                            or bd_date = to_date(to_char((sysdate + 9 / 24) - 1, 'yyyy-mm-dd'), 'yyyy-mm-dd')
                        GROUP BY bd_btime, bd_etime
                        ORDER BY bd_btime"""

    df = oracle_hook.get_pandas_df(sql_PgmSchedule)
    print(df)

    hook = RedshiftSQLHook(redshift_conn_id="redshift_conn")

    sql_GetLogUv = f"""
            WITH latest_events AS (
                -- 각 사용자의 최신 이벤트 추출
                SELECT pn                                                         as media_code,
                    uid                                                        as user_id,
                    MAX(log_time)                                              AS last_activity,
                    MAX(CASE WHEN status = 'connect' THEN st ELSE NULL END)    AS last_connect,
                    MAX(CASE WHEN status = 'disconnect' THEN st ELSE NULL END) AS last_disconnect
                FROM stoa_log.cslog_stream_daily
                WHERE log_time < GETDATE()
            --       AND log_time >= DATEADD(minute, -60, GETDATE())
                GROUP BY pn, uid),
                active_sessions AS (
                    -- 유효한 세션 판단
                    SELECT media_code,
                            user_id,
                            last_activity,
                            last_connect,
                            last_disconnect
                    FROM latest_events
                    WHERE
                    -- (1) 마지막 활동이 타임아웃 기준을 넘지 않았는지 확인
                        last_activity >= DATEADD(minute, -60, GETDATE())
                    -- (2) 마지막 이벤트가 disconnect가 아닌지 확인
                    AND (last_disconnect IS NULL OR last_connect > last_disconnect))
            -- 유효 세션 집계
            SELECT media_code,
                COUNT(DISTINCT user_id) AS unique_viewers
            FROM active_sessions
            GROUP BY media_code
            ORDER BY media_code;"""
    # SQL 쿼리 정의
    query = "SET TIME ZONE 'Asia/Seoul';"
   
    hook.run(query)

    df = hook.get_pandas_df(sql_GetLogUv)

    print(sql_GetLogUv)
    print(df)

    return 

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
    "dags_python_with_oracle_redshift_to_mysql_rake_real_session",
    default_args=default_args,
    description="Oracle PgmTapeCodeList, PgmSchedule 데이터와 Redshift Stream Log를 가져와서 MySQL vision_main_realtime_stat 테이블에 insert 하는 DAG",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 12, 12, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    fetch_data = PythonOperator(
        task_id="fetch_data_from_oracle",
        python_callable=fetch_data_from_oracle,
    )

    fetch_data 