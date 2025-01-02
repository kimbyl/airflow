from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id='dags_python_with_postgres_hook_bulk_load',
    start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    def insert_postgres(postgres_conn_id, table, file, **kwargs):
        hook = PostgresHook(postgres_conn_id)
        hook.bulk_load(table, file)
    
    insert_postgres = PythonOperator(
        task_id='insert_postgres',
        python_callable=insert_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'table': 'TbCorona19CountStatus_bulk1',
                   'file': '/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'}
    )