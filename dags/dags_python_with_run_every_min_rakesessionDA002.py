from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from datetime import datetime, timedelta

def run_every_min_rakesessionDA002(**kwargs):
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

        sql_PgmPlanModify = f"""
                WITH rbs AS (
                    SELECT
                        GOODS_CODE
                        ,media_code
                        ,to_char((order_date), 'yyyy-mm-dd hh24:mi') as order_mi
                        ,sum(order_amt) AS price
                    FROM bmtcom.TORDERDT
                    WHERE
                        order_date >= to_date(to_char(sysdate-30/(24*60)+9/24,'yyyymmdd hh24mi'),'yyyymmdd hh24mi')
                    AND order_date <= to_date(to_char(sysdate+9/24,'yyyymmdd hh24mi'),'yyyymmdd hh24mi')
                    AND order_amt > 0
                    GROUP BY GOODS_CODE,media_code,to_char((order_date), 'yyyy-mm-dd hh24:mi')
                ) ,mfs AS (
                    SELECT
                        bd_btime
                        ,tape_code
                        ,GOODS_CODE
                        ,media_code
                    FROM
                        bmtcom.VPGMMAIN
                    WHERE
                        bd_btime >= to_date(to_char(sysdate-120/(24*60)+9/24,'yyyymmdd hh24mi'),'yyyymmdd hh24mi')
                    AND bd_btime <= to_date(to_char(sysdate+10/(24*60)+9/24,'yyyymmdd hh24mi'),'yyyymmdd hh24mi')
                    GROUP BY bd_btime, tape_code, goods_code,media_code
                )
                SELECT
                    mfs.bd_btime
                    ,mfs.tape_code
                    ,rbs.order_mi
                    ,sum(rbs.price)
                FROM rbs INNER JOIN mfs
                                    ON rbs.GOODS_CODE = mfs.GOODS_CODE AND rbs.media_code = mfs.media_code
                GROUP BY mfs.bd_btime,mfs.tape_code,rbs.order_mi
                """

        df_pgm = oracle_hook.get_pandas_df(sql_PgmPlanModify)
        # print(df_pgm)

        hook = RedshiftSQLHook(redshift_conn_id="redshift_conn")
        # 타임존 설정
        set_timezone_query = "SET timezone = 'Asia/Seoul';"
        hook.run(set_timezone_query)

        for _, row in df_pgm.iterrows():
            bdBtime     = row[0]
            tapeCode    = row[1]
            initDate    = datetime.strptime(row[2], '%Y-%m-%d %H:%M')
            totalPrice  = row[3]            
            if totalPrice != 0:

                delete_query = f"""
                    -- Step 1: Delete existing rows with the same key
                    DELETE FROM vision.vision_main_realtime_stat
                    WHERE tape_code = '{tapeCode}'
                    AND data_type = 'DA002'
                    AND init_date = '{initDate}';
                    """
                insert_query = f"""
                    -- Step 2: Insert the new row
                    INSERT INTO vision.vision_main_realtime_stat (init_date, tape_code, data_type, data_value, bd_btime)
                    VALUES ('{initDate}', '{tapeCode}', 'DA002', {totalPrice}, '{bdBtime}');
                """
                sqls = [
                    # delete_query,
                    insert_query,
                ]
                for sql in sqls:
                    hook.run(sql)

        time.sleep(30)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="run_every_min_rakesessionDA002",
    default_args=default_args,
    schedule_interval="* * * * *",  # 1분마다 실행
    start_date=datetime(2024, 12, 11),
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="execute_in_every_min",
        python_callable=run_every_min_rakesessionDA002,
    )