from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

from operators.extractYtApiOperator import extractYtApiOperator
from operators.transformYtApiOperator import transformYtApiOperator
from operators.loadYtApiOperator import loadYtApiOperator

from operators.extractYtKaggleOperator import extractYtKaggleOperator
from operators.loadYtKaggleOperator import loadYtKaggleOperator

default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    #'end_date': datetime(),
    #'depends_on_past': False,

    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

# Main Function
dag = DAG(
	dag_id = "dag_youtube_api",
	default_args=default_args ,
	#schedule_interval='0 * * * *',
	schedule_interval='@daily',		
	dagrun_timeout=timedelta(minutes=60),
	description='use case of pandas  in airflow',
	start_date = days_ago(1))

start = DummyOperator(task_id='beginExecution', dag=dag)

extract = extractYtApiOperator(
            task_id='extractYtApiOperator',
            dag=dag)

transform  = transformYtApiOperator(
            task_id='transformYtApiOperator',
            dag=dag)

load = loadYtApiOperator(
            task_id='loadYtApiOperator',
            dag=dag)

extract1 = extractYtKaggleOperator(
            task_id='extract-transformYtKaggleOperator',
            dag=dag)

load1 = loadYtKaggleOperator(
            task_id='loadYtKaggleOperator',
            dag=dag)

end = DummyOperator(task_id='stopExecution', dag=dag)

start >> extract
extract >> transform
transform >> load
load >> end

start >> extract1
extract1 >> load1
load1 >> end