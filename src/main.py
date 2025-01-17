
# imports block
from airflow.models import DAG
from datetime import timedelta
import pendulum

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

# DAG arguments definition

default_args={
    'owner': 'Sathishkumartheta',
    'start_date': pendulum.today('UTC'),
    'email': ['sathishkumartheta@gmail.com'],
    'retries': 1,
    'retry_delay' : timedelta(minutes=5),
}

# DAG definition block

dag=DAG(
    dag_id='unique_id_for_dag',
    default_args=default_args,
    description='Example',
    schedule=timedelta(days=1)
)


# task definition block

task1=BashOperator(
    task_id='unique_task_id',
    bash_command='echo "Hello Woerld"',
    dag=dag,
)

task2=PythonOperator(
    task_id="pythontask",
    python_callable=lambda: print("Hello World"),
    dag=dag,
)


task3=EmailOperator(
    task_id="emailtask",
    to="sathishkumarsid@gmail.com",
    subject="Aiflow example",
    html_content="""<h1> Hello World </h1>""",
    dag=dag,
)

## Pipeline definituion

task1 >> task2 >> task3

