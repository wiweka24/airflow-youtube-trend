import csv
import glob, re
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class extractYtKaggleOperator(BaseOperator):
  
  @apply_defaults
  def __init__(self, *args, **kwargs):
   super(extractYtKaggleOperator, self).__init__(*args, **kwargs)

  def execute(self, context):

    def cleaning_text(text):
      #Remove Special Character
      text = re.sub(r'[^\w\s]', '', text)
      return text
    
    def cleaning_text_tags(text):
      #Remove Special Character
      text = re.sub(r'[^\w\s|]', '', text)
      return text
    

    path = "/opt/airflow/data/kaggle_dl/*.csv"
    glob.glob(path)
    for fname in glob.glob(path):
      fname = fname.split('/')
      csvname = fname[-1]
      csvname = csvname.split('.')
      csvname = csvname[0].split('_')
      tablename = str(csvname[0])

      data = pd.read_csv('/opt/airflow/data/kaggle_dl/' + tablename + '_youtube_trending_data.csv')
      data = data.drop('description', axis='columns')
      reallen = len(data)-1
      data = data.tail(2000)
      data['title'] = data['title'].apply(cleaning_text)
      data['channelTitle'] = data['channelTitle'].apply(cleaning_text)
      data['tags'] = data['tags'].apply(cleaning_text_tags)

      cat = pd.read_json('/opt/airflow/data/category_id.json', orient='split')

      for i in range(len(data)):
        for k in range(len(cat)):
          if(data['categoryId'][reallen - i] == cat['id'][k]):
            data['categoryId'][reallen - i] = cat['title'][k]

      data.to_csv('/opt/airflow/data/kaggle/' + tablename + '_videos.csv', sep=',' ,escapechar='\\', quoting=csv.QUOTE_NONE, encoding='utf-8' )