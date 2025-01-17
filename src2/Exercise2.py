# Import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
import requests
import pendulum
import os



# Define the path for the input and output files
input_file = os.path.abspath('data/web-server-access-log.txt')
extracted_file = os.path.abspath('data/extracted-data.txt')
transformed_file = os.path.abspath('data/transformed.txt')
output_file = os.path.abspath('data/capitalized.txt')



def download_file():
    print('inside download')
    pass


def extract():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")


def transform():
    global extracted_file, transformed_file
    print("Inside Transform")
    with open(extracted_file, 'r') as infile, open(transformed_file, 'w') as outfile:
        for line in infile:          
            processed_line = line.upper()
            outfile.write(processed_line + '\n')


def load():
    global transformed_file, output_file
    print("Inside Load")
    # Save the array to a CSV file
    with open(transformed_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            outfile.write(line + '\n')



def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)



## DAG Defualt arguments

default_args={
    'owner':'sathish',
    'start_date':pendulum.today('UTC'),
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

# DAG Definition

dag2=DAG(
    dag_id="exercise-2-dag",
    default_args=default_args,
    description="ETL from a url",
    schedule=timedelta(minutes=10),
)

### Task definitions

execute_download=PythonOperator(
    task_id="download",
    python_callable=download_file,
    dag=dag2
)

execute_extract=PythonOperator(
    task_id="extract",
    python_callable=extract,
    dag=dag2
)

execute_transform=PythonOperator(
    task_id="transform",
    python_callable=transform,
    dag=dag2
)

execute_load=PythonOperator(
    task_id="load",
    python_callable=load,
    dag=dag2

)

execute_check=PythonOperator(
    task_id="check",
    python_callable=check,
    dag=dag2
)   

### DAG pipeline

execute_download >> execute_extract >> execute_transform >> execute_load >>execute_check


    