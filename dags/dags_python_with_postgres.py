from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    
    def insert_postgres(ip, port, dbname, user, password, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=password, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                ti = kwargs.get('ti')
                dag_id = ti.dag_id
                task_id = ti.task_id
                run_id = ti.run_id
                msg = 'insert 수행'
                sql = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insert_postgres = PythonOperator(
        task_id='insert_postgres',
        python_callable=insert_postgres,
        op_args=['172.28.0.3', '5432', 'brkim', 'brkim', 'brkim']
    )

    insert_postgres
    