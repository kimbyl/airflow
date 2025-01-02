from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgressHook

with DAG(
    dag_id='dags_python_with_custom_hook_bulk_load',
    start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    def insert_postgres(postgres_conn_id, tbl, file, **kwargs):
        custom_postgres_hook = CustomPostgressHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table=tbl, file=file, delimiter=',', is_header=True, is_replace=True)

    insert_postgres = PythonOperator(
        task_id = 'insert_postgres',
        python_callable=insert_postgres,
        op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom',
                   'tbl': 'TbCorona19CountStatus_bulk2',
                   'file': '/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'}
    )