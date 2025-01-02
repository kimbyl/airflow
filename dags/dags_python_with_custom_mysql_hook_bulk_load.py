from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from hooks.custom_mysql_hook import CustomMysqlHook
from airflow import Dataset

dataset_sftp_operators_producer = Dataset("sftp_operators_producer")


with DAG(
    dag_id='dags_python_with_custom_mysql_hook_bulk_load',
    start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
    schedule=[dataset_sftp_operators_producer],
    catchup=False
) as dag:
    def insert_mysql(mysql_conn_id, table_nm, file_nm, **kwargs):
        col_list = kwargs.get('col_list') or []
        dtype = kwargs.get('dtype') or None
        custom_mysql_hook = CustomMysqlHook(mysql_conn_id=mysql_conn_id)
        custom_mysql_hook.bulk_load(table_name=table_nm, file_name=file_nm, delimiter=',', col_list=col_list,
                                    is_header=False, is_replace=False, dtype=dtype)
        
    insert_mysql = PythonOperator(
        task_id='insert_mysql',
        python_callable=insert_mysql,
        op_kwargs={
                'mysql_conn_id': 'mysql_aurora',
                'table_nm': 'pgm_nielson',
                'file_nm': '/opt/airflow/data/{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds_nodash }}A.csv',                 
                'col_list': ['ch_nm', 'ch', 'pgm_nm', 'start_time', 'end_time', 
                     'household_viewership', 'household_viewers', 
                     'personal_viewership', 'personal_viewers',
                     'male_viewership', 'male_viewers',
                     'female_viewership', 'female_viewers',
                     'under10s_viewership', 'under10s_viewers',
                     'teenagers_viewership', 'teenagers_viewers',
                     '20s_viewership', '20s_viewers',
                     '30s_viewership', '30s_viewers',
                     '40s_viewership', '40s_viewers',
                     '50s_viewership', '50s_viewers',
                     'over60s_viewership', 'over60s_viewers',
                     ],
                 'dtype': {'start_time': str, 'end_time': str}
        }
    )