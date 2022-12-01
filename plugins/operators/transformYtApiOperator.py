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
      return text

    def cleaning_text_tags(text):
      #Remove Special Character
      text = re.sub(r'[^\w\s|]', '', text)
      return text

    path = "/opt/airflow/data/api/*.csv"
    glob.glob(path)
    for fname in glob.glob(path):
      fname = fname.split('/')
      csvname = fname[-1]
      csvname = csvname.split('.')
      tablename = str(csvname[0])

      data = pd.read_csv('/opt/airflow/data/api/' + tablename + '.csv')
      data['title'] = data['title'].apply(cleaning_text)
      data['channelTitle'] = data['channelTitle'].apply(cleaning_text)
      data['tags'] = data['tags'].apply(cleaning_text_tags)

      cat = pd.read_json('/opt/airflow/data/category_id.json', orient='split')

      for i in range(len(data)):
        for k in range(len(cat)):
          if(data['categoryId'][i] == cat['id'][k]):
            data['categoryId'][i] = cat['title'][k]

      data.to_csv('/opt/airflow/data/api/' + tablename + '.csv', sep=',' ,escapechar='\\', quoting=csv.QUOTE_NONE, encoding='utf-8' )