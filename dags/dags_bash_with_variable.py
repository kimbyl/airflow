import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 10, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    var_value = Variable.get("sample_key")

    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command=f"echo {var_value}",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo {{var.value.sample_key}}",
    )
