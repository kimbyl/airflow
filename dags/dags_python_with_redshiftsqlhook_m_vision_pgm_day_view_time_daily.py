from airflow import DAG
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator
import pandas as pd
import pendulum

redshift_conn_id = 'redshift_conn'

# def load_data_from_redshift(**kwargs):
#     """RedshiftSQLHook의 get_sqlalchemy_engine()을 사용하여 Redshift 데이터 가져오기"""
#     redshift_hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)

#     # SQLAlchemy 엔진 가져오기
#     engine = redshift_hook.get_sqlalchemy_engine()

#     # SQL 쿼리 정의
#     query = "SELECT * FROM BMTCOM.TORDERPROMO LIMIT 10"

#     # Pandas를 사용하여 Redshift에서 데이터 가져오기
#     with engine.connect() as connection:
#         result = connection.execute(query)
#         for row in result:
#             print(row)
#         # df = pd.read_sql(query, connection)

#     # 데이터 출력
#     # print(df)

def call_stored_procedure(**kwargs):
    from dateutil import relativedelta

    end_date = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime("%Y-%m-%d")

    hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)

    # SQL 쿼리 정의
    querys = ["SET TIME ZONE 'Asia/Seoul';",
              f"""
                delete from stoa_on.m_vision_pgm_day_view_time_daily
                where bd_date = '{end_date}';
              """,
              f"""
                insert into stoa_on.m_vision_pgm_day_view_time_daily ( bd_date
                                                                    , bd_btime
                                                                    , tape_code
                                                                    , base_mi
                                                                    , view_time_cd
                                                                    , view_cnt)
                select c.bd_date
                    , c.bd_btime
                    , c.tape_code
                    , case
                        when a.enter_time <= c.bd_btime then 0
                        else datediff('minute', c.bd_btime, a.enter_time)
                    end as      base_mi
                    , case
                        when a.view_time < 1 then '0.1초미만'
                        when a.view_time >= 1 and a.view_time < 10 then '1.1-10초'
                        when a.view_time >= 10 and a.view_time < 30 then '2.10-30초'
                        when a.view_time >= 30 and a.view_time < 60 then '3.30초-1분'
                        when a.view_time >= 60 and a.view_time < 120 then '4.1분-2분'
                        when a.view_time >= 120 and a.view_time < 180 then '5.2분-3분'
                        when a.view_time >= 180 and a.view_time < 240 then '6.3분-4분'
                        when a.view_time >= 240 and a.view_time < 300 then '7.4분-5분'
                        when a.view_time >= 300 then '8.5분이상'
                    end as      view_time_cd
                    , count(*) view_cnt
                from (select log_dt
                        , pn
                        , sid
                        , min(start_time)                                                          as enter_time
                        , nvl(max(end_time), max(start_time))                                      as exit_time
                        , datediff('second', min(start_time), nvl(max(end_time), max(start_time))) as view_time
                    from stoa_log.cslog_current_daily
                    where log_dt = '{end_date}'
                    group by log_dt, pn, sid) a
                , stoa_on.d_dhms b
                , (select bd_date, bd_btime, media_code, tape_code
                    from bmtcom.vpgmmain v
                    where bd_date = '{end_date}'
                    group by bd_date, bd_btime, media_code, tape_code) c
                where a.enter_time = b.dhms
                and a.exit_time >= c.bd_btime -- 방송시작시간 
                and a.enter_time < dateadd('hour', 1, c.bd_btime)
                and a.pn = c.media_code
                group by c.bd_date, c.bd_btime, c.tape_code
                    , case
                            when a.enter_time <= c.bd_btime then 0
                            else datediff('minute', c.bd_btime, a.enter_time)
                    end
                    , case
                            when a.view_time < 1 then '0.1초미만'
                            when a.view_time >= 1 and a.view_time < 10 then '1.1-10초'
                            when a.view_time >= 10 and a.view_time < 30 then '2.10-30초'
                            when a.view_time >= 30 and a.view_time < 60 then '3.30초-1분'
                            when a.view_time >= 60 and a.view_time < 120 then '4.1분-2분'
                            when a.view_time >= 120 and a.view_time < 180 then '5.2분-3분'
                            when a.view_time >= 180 and a.view_time < 240 then '6.3분-4분'
                            when a.view_time >= 240 and a.view_time < 300 then '7.4분-5분'
                            when a.view_time >= 300 then '8.5분이상'
                    end;
              """
              ]
    
    for query in querys:
        hook.run(query)

# def truncate_tables(**kwargs):
#     hook = RedshiftSQLHook(redshift_conn_id=redshift_conn_id)
#     # SQL 쿼리 정의
#     querys = ["truncate table stoa_log.cslog_current_work_01;", 
#               "truncate table stoa_log.cslog_current_daily;"]
#     for query in querys:
#         hook.run(query)

# DAG 정의
default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="dags_python_with_redshiftsqlhook_m_vision_pgm_day_view_time_daily",
    default_args=default_args,
    description="일별 Vison용 home 방송편성표 시청자 분석",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 11, 27, tz='Asia/Seoul'),
    catchup=False
) as dag:
    

    call_stored_procedure = PythonOperator(
        task_id="call_stored_procedure",
        python_callable=call_stored_procedure,
    )

    call_stored_procedure