from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow import Dataset

dataset_sftp_operators_producer = Dataset("sftp_operators_producer")
with DAG(
    "sftp_operators",
    start_date=pendulum.datetime(2024, 10, 1, tz="Asia/Seoul"),
    schedule="0 10 * * *",
    catchup=False,
) as dag:
    
    wait_for_file = SFTPSensor(
        task_id="check-for-file",
        sftp_conn_id="sftp_conn_id",
        path='/skstoa/{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds_nodash }}A.CSV',
        poke_interval=60,
        mode='reschedule')
    
    download_file = SFTPOperator(
        task_id="download-file",
        ssh_conn_id="sftp_conn_id",
        local_filepath='/opt/airflow/data/{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds_nodash }}A.CSV',
        remote_filepath='/skstoa/{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds_nodash }}A.CSV',
        operation="get",
        create_intermediate_dirs=True,
        outlets=[dataset_sftp_operators_producer]
    )
    
    wait_for_file >> download_file


