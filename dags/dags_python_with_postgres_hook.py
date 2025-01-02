from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_postgres_hook',
    start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    def insert_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing

        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                ti = kwargs.get('ti')
                dag_id = ti.dag_id
                task_id = ti.task_id
                run_id = ti.run_id
                msg = 'hook insert 수행'
                sql = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()
    
    insert_postgres_with_hook = PythonOperator(
        task_id='insert_postgres_with_hook',
        python_callable=insert_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
    )

    insert_postgres_with_hook