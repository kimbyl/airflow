
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id="dags_simple_http_operator",
    schedule=None,
    start_date=pendulum.datetime(2024, 10, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    '''서울시 공공자전거 실시간 대여정보'''
    tb_seoul_bike_list = SimpleHttpOperator(
        task_id = 'tb_seoul_bike_list',
        http_conn_id = 'openapi.seoul.go.kr',
        endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/bikeList/1/5/',
        method = 'GET',
        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'},
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_seoul_bike_list')

        import json
        from pprint import pprint
        
        pprint(json.loads(rslt))


    tb_seoul_bike_list >> python_2()