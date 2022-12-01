from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

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
	dag_id = "dag_youtube_kaggle",
	default_args=default_args ,
	#schedule_interval='0 * * * *',
	schedule_interval='@once',		
	dagrun_timeout=timedelta(minutes=60),
	description='use case of pandas  in airflow',
	start_date = days_ago(1))

def getDataToLocal():

  
  def install_and_import(package):
    import importlib
    try:
        importlib.import_module(package)
    except ImportError:
        import pip
        pip.main(['install', package])
    finally:
        globals()[package] = importlib.import_module(package)
  install_and_import('opendatasets')

  opendatasets.download(
    "https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset")

getDataToLocal = PythonOperator(task_id='getDataToLocal', python_callable=getDataToLocal, dag=dag)

getDataToLocal

if __name__ == '__main__ ':
  dag.cli()