from datetime import timedelta

#dag imports

from airflow.models import DAG
from airflow.operators.python import PythonOperator
import pendulum

###Global variables
import os

input_file = os.path.abspath('/etc/passwd')  # or provide full path
extracted_file = os.path.abspath('data/extracted.txt')  # store in data directory
transformed_file = os.path.abspath('data/transformed.txt')
output_file = os.path.abspath( 'data/data_for_analytics.csv')

def extract():
    global input_file,extracted_file
    print("inside Extract")
    with open(input_file,'r') as infile, open(extracted_file, 'w') as outfile:
        for line in infile:
            fields=line.split(':')
            if(len(fields)>=6):
                field_1=fields[0]
                field_3=fields[2]
                field_6=fields[5]
                outfile.write(field_1+":"+field_3+":"+field_6+"\n")

def transform():
    global extracted_file,transformed_file
    print("Inside transform")
    with open(extracted_file,"r") as infile, open(transformed_file, "w") as outfile:
        for line in infile:
            processed_line=line.replace(":",",")
            outfile.write(processed_line+"\n")

def load():
    global transformed_file,output_file
    print("Inside load")
    with open(transformed_file, "r") as infile, open(output_file, "w") as outfile:
        for line in infile:
            outfile.write(line + "\n")


def check():
    global output_file
    print("inside check")
    with open(output_file, "r") as infile:
        for line in infile:
            print(line)


### DAG default arguments
default_args={
    'owner':'Sathish',
    'start_date': pendulum.today('UTC'),
    'retries': 1,
    'retry_delay' : timedelta(days=1)
}

### DAG Definition

dag=DAG(
    dag_id='exercise-1-dag',
    default_args=default_args,
    description='Python Operator ETL DAG',
    schedule=timedelta(minutes=1)
)

### tASK DEFINITIONS

execute_extract=PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

execute_transform=PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)


execute_load=PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)


execute_check=PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)

### Pipeline definition
execute_extract >> execute_transform >> execute_load >> execute_check

        

