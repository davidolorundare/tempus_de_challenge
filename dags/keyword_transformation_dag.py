"""Tempus Bonus challenge - Airflow data pipeline definition.

Describes a data pipeline that would fetch data from the News API based on
four keywords - 'Tempus Labs', 'Eric Lefkofsky', 'Cancer', and 'Immunotherapy'.
The data is transformed into a tabular structure, and finally stored the an AWS
S3 Bucket.
"""


from datetime import datetime, timedelta

from airflow import DAG
from airflow import settings
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Connection
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator

import challenge as c
import config


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 4),
    'email': ['david.o@ieee.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}


# path, relative to AIRFLOW_HOME, to the news folder which
# stores data from the News API
NEWS_DIRECTORY = "usr/local/airflow/tempdata/tempus_bonus_challenge_dag/news/"

# NEED TO MAINTAIN SECRECY OF API KEYS
# https://12factor.net/config
# this should NOT be hardcoded (at least put it in an environment variable)
# should be replaced with the user's own generate News API Key
API_KEY = config.NEWS_API_KEY

# Connection object for the News API endpoints
conn_news_api = Connection(conn_id="newsapi",
                           conn_type="HTTP",
                           host="https://newsapi.org")

# Connection object for local filesystem access
conn_filesystem = Connection(conn_id="filesys",
                             conn_type="File (path)",
                             extra=None)

# Create connection object
session = settings.Session()
session.add(conn_news_api)
session.add(conn_filesystem)
session.commit()


# DAG Object
dag = DAG('tempus_bonus_challenge_dag',
          default_args=default_args,
          schedule_interval='0 01 * * *',
          catchup=False)

# define workflow tasks
# begin workflow
start_task = DummyOperator(task_id='start', dag=dag)

# use an alias since the length of the real function call is more than
# PEP8's 79 line-character limit
storage_func_alias = c.FileStorage.create_storage
headlines_func_alias = c.NetworkOperations.get_news_keyword_headlines
transform_func_alias = c.TransformOperations.transform_headlines_to_csv
upload_func_alias = c.UploadOperations.upload_csv_to_s3

# create a folder for storing retrieved data on the local filesystem
datastore_creation_task = PythonOperator(task_id='create_storage_task',
                                         provide_context=True,
                                         python_callable=storage_func_alias,
                                         retries=3,
                                         dag=dag)

# retrieve all top news headlines for specific keywords
# Need to make four SimpleHTTPOperator calls run in parallel
news_kw1_task = SimpleHttpOperator(endpoint='v2/top-headlines?',
                                   method='GET',
                                   data={'q': 'Tempus Labs',
                                         'apiKey': API_KEY},
                                   response_check=headlines_func_alias,
                                   http_conn_id='newsapi',
                                   task_id='get_headlines_first_kw_task',
                                   dag=dag)

news_kw2_task = SimpleHttpOperator(endpoint='v2/top-headlines?',
                                   method='GET',
                                   data={'q': 'Eric Lefkofsky',
                                         'apiKey': API_KEY},
                                   response_check=headlines_func_alias,
                                   http_conn_id='newsapi',
                                   task_id='get_headlines_second_kw_task',
                                   dag=dag)

news_kw3_task = SimpleHttpOperator(endpoint='v2/top-headlines?',
                                   method='GET',
                                   data={'q': 'Cancer',
                                         'apiKey': API_KEY},
                                   response_check=headlines_func_alias,
                                   http_conn_id='newsapi',
                                   task_id='get_headlines_third_kw_task',
                                   dag=dag)

news_kw4_task = SimpleHttpOperator(endpoint='v2/top-headlines?',
                                   method='GET',
                                   data={'q': 'Immunotherapy',
                                         'apiKey': API_KEY},
                                   response_check=headlines_func_alias,
                                   http_conn_id='newsapi',
                                   task_id='get_headlines_fourth_kw_task',
                                   dag=dag)

# detect existence of retrieved news data
file_exists_sensor = FileSensor(filepath=NEWS_DIRECTORY,
                                fs_conn_id="filesys",
                                poke_interval=5,
                                soft_fail=True,
                                timeout=3600,
                                task_id='file_sensor_task',
                                dag=dag)

# retrieve all of the top headlines
extract_headlines_task = PythonOperator(task_id='extract_headl_kw_task',
                                        provide_context=True,
                                        python_callable=headlines_func_alias,
                                        retries=3,
                                        dag=dag)

# transform the data, resulting in a flattened csv
flatten_csv_task = PythonOperator(task_id='transform_to_csv_kw_task',
                                  provide_context=True,
                                  python_callable=transform_func_alias,
                                  retries=3,
                                  dag=dag)

# upload the flattened csv into my S3 bucket
upload_csv_task = PythonOperator(task_id='upload_csv_to_s3_kw_task',
                                 provide_context=True,
                                 python_callable=upload_func_alias,
                                 retries=3,
                                 dag=dag)

# end workflow
end_task = DummyOperator(task_id='end', dag=dag)


# arrange the workflow tasks
# create folder that acts as 'staging area' to store retrieved
# data before processing. In a production system this would be
# a real database.
start_task >> datastore_creation_task >> news_kw1_task >> file_exists_sensor

# make news api calls with the four keywords and ensure the
# data has been retrieved before beginning the ETL process.
datastore_creation_task >> news_kw2_task >> file_exists_sensor
datastore_creation_task >> news_kw3_task >> file_exists_sensor
datastore_creation_task >> news_kw4_task >> file_exists_sensor

# all the news sources are retrieved, the top headlines
# extracted, and the data transform by flattening into CSV.
file_exists_sensor >> extract_headlines_task >> flatten_csv_task

# perform a file transfer operation, uploading the CSV data
# into S3 from local.
flatten_csv_task >> upload_csv_task >> end_task
