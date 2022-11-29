import airflow
import requests
import json
import pandas as pd
import psycopg2 as pg
import csv
import sys, time, os, argparse
import glob
import re

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

args={'owner': 'airflow'}

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

def getDataToLocal():

  snippet_features = ["title",
                    "publishedAt",
                    "channelId",
                    "channelTitle",
                    "categoryId"]
  unsafe_characters = ['\n', '"']
  header = ["video_id"] + snippet_features + ["trending_date", "tags", "view_count", "likes", "comment_count"] 
                                              # "thumbnail_link", "description"
  # def setup(api_path, code_path):
  #     with open(api_path, 'r') as file:
  #         api_key = file.readline()
  #     with open(code_path) as file:
  #         country_codes = [x.rstrip() for x in file]
  #     return api_key, country_codes

  def prepare_feature(feature):
      for ch in unsafe_characters:
          feature = str(feature).replace(ch, "")
      return f'"{feature}"'

  def api_request(page_token, country_code):
      request_url = f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}"
      request = requests.get(request_url)
      if request.status_code == 429:
          print("Temp-Banned due to excess requests, please wait and continue later")
          sys.exit()
      return request.json()

  def get_tags(tags_list):
      return prepare_feature("|".join(tags_list))

  def get_videos(items):
      lines = []
      for video in items:
          if "statistics" not in video:
              continue
          video_id = prepare_feature(video['id'])
          snippet = video['snippet']
          statistics = video['statistics']
          features = [prepare_feature(snippet.get(feature, "")) for feature in snippet_features]
          description = snippet.get("description", "")
          thumbnail_link = snippet.get("thumbnails", dict()).get("default", dict()).get("url", "")
          trending_date = time.strftime("%y.%d.%m")
          tags = get_tags(snippet.get("tags", ["[none]"]))
          view_count = statistics.get("viewCount", 0)
          
          if 'likeCount' in statistics:
              likes = statistics['likeCount']
              # dislikes = statistics['dislikeCount']
          else:
              likes = 0
          if 'commentCount' in statistics:
              comment_count = statistics['commentCount']
          else:
              comment_count = 0
          line = [video_id] + features + [prepare_feature(x) for x in [trending_date, tags, view_count, likes, comment_count]]
          lines.append(",".join(line))
      return lines

  def get_pages(country_code, next_page_token="&"):
      country_data = []
      while next_page_token is not None:
          video_data_page = api_request(next_page_token, country_code)
          
          if video_data_page.get('error'):
              print(video_data_page['error'])
          next_page_token = video_data_page.get("nextPageToken", None)
          next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token
          items = video_data_page.get('items', [])
          country_data += get_videos(items)
      return country_data

  def write_to_file(country_code, country_data):
      print(f"Writing {country_code} data to file...")
      if not os.path.exists(output_dir):
          os.makedirs(output_dir)
      with open(f"{output_dir}/{country_code}_videos.csv", "w+", encoding='utf-8') as file:
          for row in country_data:
              file.write(f"{row}\n")

  def get_data():
      for country_code in country_codes:
          country_data = [",".join(header)] + get_pages(country_code)
          write_to_file(country_code, country_data)

  output_dir = "/opt/airflow/data/"
  api_key = "AIzaSyAyC91r6h-lGuNp6hq8mhcO0fpkx4LrO-k"
  country_codes = ["ID", "EG", "GB"]
  get_data()

def transform():

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

def creatableLoad():

  try:
      dbconnect = pg.connect(
          "dbname='airflow' user='airflow' host='apacheairflow-postgres-1' password='airflow'"
      )
  except Exception as error:
      print(error)

  path = "/opt/airflow/data/*.csv"
  glob.glob(path)
  for fname in glob.glob(path):
    fname = fname.split('/')
    csvname = fname[-1]
    csvname = csvname.split('.')
    tablename = str(csvname[0])

    # create the table if it does not already exist
    cursor = dbconnect.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS youtube_""" + tablename + """ (
            index varchar(500),
            video_id varchar(500), 
            title varchar(500),
            publishedAt varchar(500),
            channelId varchar(500),
            channelTitle varchar(500),
            categoryId varchar(500),
            trending_date varchar(500),
            tags varchar(500),
            view_count varchar(500),
            likes varchar(500),
            comment_count varchar(500)
        );
        
        TRUNCATE TABLE youtube_""" + tablename + """;
    """
    )
    dbconnect.commit()
    
    # insert each csv row as a record in our database
    with open('/opt/airflow/data/' + tablename + '.csv', 'r') as f:
        next(f)  # skip the first row (header)     
        for row in f:
            cursor.execute("""
                INSERT INTO youtube_""" + tablename + """
                VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')
            """.format(
            row.split(",")[0],
            row.split(",")[1],
            row.split(",")[2],
            row.split(",")[3],
            row.split(",")[4],
            row.split(",")[5],
            row.split(",")[6],
            row.split(",")[7],
            row.split(",")[8],
            row.split(",")[9],
            row.split(",")[10],
            row.split(",")[11])
            )
    dbconnect.commit()


# Main Function
dag_pandas = DAG(
	dag_id = "dag_youtube_api",
	default_args=default_args ,
	# schedule_interval='0 0 * * *',
	schedule_interval='@once',	
	dagrun_timeout=timedelta(minutes=60),
	description='use case of pandas  in airflow',
	start_date = airflow.utils.dates.days_ago(1))

getDataToLocal = PythonOperator(task_id='getDataToLocal', python_callable=getDataToLocal, dag=dag_pandas)
transform = PythonOperator(task_id='transform', python_callable=transform, dag=dag_pandas)
creatableLoad = PythonOperator(task_id='creatableLoad', python_callable=creatableLoad, dag=dag_pandas)

getDataToLocal >> transform >>creatableLoad

if __name__ == '__main__ ':
  dag_pandas.cli()