from airflow import DAG
import pendulum
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator

default_args = {
    'owner': 'airflow',
}

def parse_csv(filepath):
    import csv

    with open(filepath, newline="") as file:
        yield from csv.reader(file)

with DAG(
    dag_id='dags_python_with_s3_to_sql_operator',
    start_date=pendulum.datetime(2024, 10, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False,
) as dag:
    load_data = S3ToSqlOperator(
        task_id='load_s3_to_aurora',
        schema='dep',
        table='nielson',
        s3_bucket='skstoadev-nielsen',
        s3_key='{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1))| ds_nodash}}A.csv',
        aws_conn_id='aws_conn',
        sql_conn_id='mysql_aurora',
        column_list=['ch_nm', 'ch', 'pgm_nm', 'start_time', 'end_time', 
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
        parser=parse_csv,
    ) 
