from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from datetime import datetime, timedelta

def run_every_min_rakesessionDA001(**kwargs):
    from dateutil import relativedelta
    import time
    import pandas as pd

    initDate = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y-%m-%d %H:%M:00')
    sDate = datetime.strptime(initDate, '%Y-%m-%d %H:%M:00')
    nowDate = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y%m%d%H%M')

    beforeDate = (kwargs.get('data_interval_end').in_timezone('Asia/Seoul') + relativedelta.relativedelta(days=-1)).strftime("%Y-%m-%d")
    endDate = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y-%m-%d')

    print(f"initDate: {initDate}, sDate: {sDate}, nowDate: {nowDate}, beforeDate: {beforeDate}, endDate: {endDate}")

    for i in range(1):  # 1분 동안 5초 간격으로 실행
        print(f"Task executed at iteration {i + 1}")

        oracle_hook = OracleHook(oracle_conn_id="oracle_conn")

        sql_PgmTapeCodeList = f"""SELECT to_char(bd_btime, 'yyyymmddhh24mi') as bd_btime,
                                    media_code,
                                    channel_code,
                                    tape_code
                                FROM bmtcom.tmultiframesche
                                WHERE bd_date = to_date('{beforeDate}', 'yyyy-mm-dd')
                                or bd_date = to_date('{endDate}', 'yyyy-mm-dd')"""

        df_codelist = oracle_hook.get_pandas_df(sql_PgmTapeCodeList)
        # print(df_codelist)
# 202412150031       TV00      0000002  0000026036
# 202412150131       TV00      0000002  0000026316
# 202412150211       TV00      0000002  0000026063
# 202412150251       TV00      0000002  0000026076
# ...
# 202412162031       TV50      0000001  0000026290
# 202412162131       TV50      0000001  0000026554
# 202412162231       TV50      0000001  0000026317
# 202412162331       TV50      0000001  0000026265

        sql_PgmSchedule =f"""SELECT to_char(bd_btime, 'yyyymmddhh24mi') as bd_btime,
                            to_char(bd_etime, 'yyyymmddhh24mi') as bd_etime
                            FROM bmtcom.vpgmmain
                            WHERE bd_date = to_date('{beforeDate}', 'yyyy-mm-dd')
                                or bd_date = to_date('{endDate}', 'yyyy-mm-dd')
                            GROUP BY bd_btime, bd_etime
                            ORDER BY bd_btime"""

        df_schedule = oracle_hook.get_pandas_df(sql_PgmSchedule)
        # print(df_schedule)
# 202412150031                       202412150131
# 202412150131                       202412150211
# 202412150211                       202412150251
# 202412150251                       202412150331
# ...
# 202412162031                       202412162131
# 202412162131                       202412162231
# 202412162231                       202412162331
# 202412162331                       202412170031

        hook = RedshiftSQLHook(redshift_conn_id="redshift_conn")

        # 타임존 설정과 SELECT 구문 실행
        set_timezone_query = "SET timezone = 'Asia/Seoul';"
        select_query = f"""
                WITH latest_events AS (
                    -- 각 사용자의 최신 이벤트 추출
                    SELECT pn                                                      as media_code,
                        uid                                                        as user_id,
                        MAX(log_time)                                              AS last_activity,
                        MAX(CASE WHEN status = 'connect' THEN st ELSE NULL END)    AS last_connect,
                        MAX(CASE WHEN status = 'disconnect' THEN st ELSE NULL END) AS last_disconnect
                    FROM stoa_log.cslog_stream_daily
                    WHERE log_time < '{initDate}'::timestamp AT TIME ZONE 'Asia/Seoul'
                       AND log_time >= DATEADD(minute, -60, '{initDate}')::timestamp AT TIME ZONE 'Asia/Seoul'
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
                            last_activity >= DATEADD(minute, -60, '{initDate}')::timestamp AT TIME ZONE 'Asia/Seoul'
                        -- (2) 마지막 이벤트가 disconnect가 아닌지 확인
                        AND (last_disconnect IS NULL OR last_connect > last_disconnect))
                -- 유효 세션 집계
                SELECT media_code,
                    COUNT(DISTINCT user_id) AS unique_viewers
                FROM active_sessions
                GROUP BY media_code
                ORDER BY media_code;"""

        hook.run(set_timezone_query)
        df_uv = hook.get_pandas_df(select_query)
        
        print(df_uv)
# TV01            1419
# TV02             384
# TV04            2169
# TV06            1203
# TV07             993
# TV08             291
# TV09               5
# TV10              20
# TV11              29
# TV12             397
# TV13              17
# TV15               8
# TV17              44
# TV36              12
# TV40              14
# TV42            1050
# TV48            1238

        bd_btime = df_schedule.iloc[:, 0]
        bd_btime_string = bd_btime[bd_btime <= nowDate].max()
        _BD_BDATE = datetime.strptime(bd_btime_string, '%Y%m%d%H%M')

        print(f"_BD_BDATE: {bd_btime_string}")
        df_filterd = df_codelist[df_codelist.BD_BTIME == bd_btime_string]

        # df와 df_uv를 media_code 기준으로 병합
        merged_df = pd.merge(df_filterd, df_uv, left_on="MEDIA_CODE", right_on="media_code", how="left")

        # tape_code별 unique_viewers 합산
        result = merged_df.groupby("TAPE_CODE")["unique_viewers"].sum().reset_index()
        print(result)

        for _, row in result.iterrows():
                
            insert_query = f"""
                insert into vision.vision_main_realtime_stat(init_date,tape_code,data_type,data_value,bd_btime)
                values ('{initDate}','{row['TAPE_CODE']}','DA001',{row['unique_viewers']},'{_BD_BDATE}');
            """
            hook.run(insert_query)

        time.sleep(30)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="run_every_min_rakesessionDA001",
    default_args=default_args,
    schedule_interval="* * * * *",  # 1분마다 실행
    start_date=datetime(2024, 12, 11),
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="execute_in_every_min",
        python_callable=run_every_min_rakesessionDA001,
    )