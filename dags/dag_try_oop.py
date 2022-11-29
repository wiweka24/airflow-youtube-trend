from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

from operators.extractYtApiOperator import extractYtApiOperator
from operators.transformYtApiOperator import transformYtApiOperator
from operators.loadYtApiOperator import loadYtApiOperator

default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

# Main Function
dag_pandas = DAG(
	dag_id = "dag_try_oop",
	default_args=default_args ,
	#schedule_interval='0 * * * *',
	schedule_interval='@once',		
	dagrun_timeout=timedelta(minutes=60),
	description='use case of pandas  in airflow',
	start_date = days_ago(1))

start = DummyOperator(task_id='beginExecution', dag=dag_pandas)

extract = extractYtApiOperator(
            task_id='extractYtApiOperator',
            dag=dag_pandas)

transform  = transformYtApiOperator(
              task_id='transformYtApiOperator',
              dag=dag_pandas)

load = loadYtApiOperator(
        task_id='loadYtApiOperator',
        dag=dag_pandas)

end = DummyOperator(task_id='stopExecution', dag=dag_pandas)

start >> extract >> transform >> load >> end