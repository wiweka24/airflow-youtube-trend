#datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

import urllib
import json                 # Used to load data into JSON format
from pprint import pprint
import pandas as pd

# initializing the default arguments
default_args = {
		'owner': 'wiweka',
		'start_date': datetime(2022, 1, 1),
		'retries': 3,
		'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG object
hello_world_dag = DAG('hello_world_dag',
		default_args=default_args,
		description='Hello World DAG',
		schedule_interval='* * * * *', 
		catchup=False,
		tags=['example, helloworld']
)

# python callable function
def print_hello():
  url = "https://data.sa.gov.au/data/api/3/action/datastore_search?resource_id=fec742c1-c846-4343-a9f1-91c729acd097&limit=5&q=title:jones"
  response = urllib.request.urlopen(url)
  print(response)
  df = pd.read_json(response)
  df.to_csv("output.csv")
  return print(response)

# Creating first task
start_task = DummyOperator(task_id='start_task', dag=hello_world_dag)

# Creating second task
hello_world_task = PythonOperator(task_id='hello_world_task', python_callable=print_hello, dag=hello_world_dag)

# Creating third task
end_task = DummyOperator(task_id='end_task', dag=hello_world_dag)

# Set the order of execution of tasks. 
start_task >> hello_world_task >> end_task