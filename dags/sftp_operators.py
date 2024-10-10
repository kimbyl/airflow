from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
with DAG(
    "sftp_operators",
    start_date=datetime(2024,5,1),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    
    wait_for_file = SFTPSensor(
        task_id="check-for-file",
        sftp_conn_id="sftp_conn_id",
        path='/skstoa/{{ ds_nodash }}A.CSV',
        poke_interval=10)
    
    download_file = SFTPOperator(
        task_id="download-file",
        ssh_conn_id="sftp_conn_id",
        local_filepath='/opt/airflow/data/{{ ds_nodash }}A.CSV',
        remote_filepath='/skstoa/{{ ds_nodash }}A.CSV',
        operation="get",
        create_intermediate_dirs=True
    )
    
    wait_for_file >> download_file


