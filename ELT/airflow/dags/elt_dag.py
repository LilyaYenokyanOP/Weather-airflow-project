from datetime import datetime, timedelta
from airflow import DAG
from docker.types import Mount
from airflow.operators.python3.operators import PythonOperator
from airflow.operators.python3.bash import BashOperator
from airflow.operators.python3.docker import DockerOperator
import subprocess

default_args={
    "owner":"airflow",
    "depends_on": False,
    "email_on_failure": False,
    "email_on_retry": False,

}

# in order to run the script
def run_elt_script():
    script_path='/opt/airflow/elt_script.py'
    result = subprocess.run(["python3", script_path], capture_output=True, text=True)

    # error checks
    if(result.returncode != 0):
        raise Excepton(f"Script failed with error: {result.stderr}")
    else:
        print(result.stdout)

# attached to a dag
dag=DAG(
    'elt_and_dbt',
    default_args=default_args,
    description="An ELT workflow with dbt"
    start_date=datetime(2026,4,19),
    catchup=False
)

# tasks
t1 = PythonOperator(
    task_id="run_elt_script",
    python_callable=run_elt_script,
    dag=dag
)

# write order how we want to run it
#t1 >> t2





