import csv
import glob, re
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class transformYtApiOperator(BaseOperator):
  
  @apply_defaults
  def __init__(self, *args, **kwargs):
   super(transformYtApiOperator, self).__init__(*args, **kwargs)

  def execute(self, context):

    def cleaning_text(text):
      #Remove Special Character
      text = re.sub(r'[^\w\s]', '', text)
      text = text.strip()
      text = text.lower()
      return text

    path = "/opt/airflow/data/*.csv"
    glob.glob(path)
    for fname in glob.glob(path):
      fname = fname.split('/')
      csvname = fname[-1]
      csvname = csvname.split('.')
      tablename = str(csvname[0])

      data = pd.read_csv('/opt/airflow/data/' + tablename + '.csv')
      data['title'] = data['title'].apply(cleaning_text)
      data['tags'] = data['tags'].apply(cleaning_text)
      data['channelTitle'] = data['channelTitle'].apply(cleaning_text)
      data.to_csv('/opt/airflow/data/' + tablename + '.csv', sep=',' ,escapechar='\\', quoting=csv.QUOTE_ALL, encoding='utf-8' )