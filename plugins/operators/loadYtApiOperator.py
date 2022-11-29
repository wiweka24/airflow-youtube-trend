import glob
import psycopg2 as pg
import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class loadYtApiOperator(BaseOperator):
  
  @apply_defaults
  def __init__(self, *args, **kwargs):
    super(loadYtApiOperator, self).__init__(*args, **kwargs)

  def execute(self, context):
    def checkdate(dbconnect, tablename):
      testcursor = dbconnect.cursor()
      testcursor.execute("""
        SELECT DISTINCT trending_date
        FROM youtube_""" + tablename + """;
      """
      )

      data = pd.read_csv('/opt/airflow/data/' + tablename + '.csv')
      y = str(data["trending_date"][0])

      check = True
      myresult = testcursor.fetchall()
      for x in myresult:
        z = x[0].replace('"', "")
        if (z == y):
          check = False
      
      return(check)

    def addtodb():
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
        """
        )
        dbconnect.commit()
        
        check = checkdate(dbconnect, tablename)
        if(check):
          
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

    addtodb()