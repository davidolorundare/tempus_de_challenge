"""Tempus Bonus challenge - Airflow data pipeline definition.

Describes a data pipeline that would fetch data from the News API based on
four keywords - 'Tempus Labs', 'Eric Lefkofsky', 'Cancer', and 'Immunotherapy'.
The data is transformed into a tabular structure, and finally stored the an AWS
S3 Bucket.
"""

import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow import settings
from airflow.models import Connection
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator

import challenge as c


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
NEWS_DIRECTORY = "tempdata/tempus_bonus_challenge_dag/news/"
AIRFLOW_HOME = os.environ['HOME']

# NEED TO MAINTAIN SECRECY OF API KEYS
# https://12factor.net/config
# this should NOT be hardcoded (put it in an environment variable)
API_KEY = '68ce2435405b42e5b4a90080249c6962'

# Connection object for the News API endpoints
conn_news_api = Connection(conn_id="newsapi",
                           conn_type="HTTP",
                           host="https://newsapi.org")

# Connection object for local filesystem access
conn_filesystem = Connection(conn_id="filesys",
                             conn_type="File (path)",
                             extra={"path": AIRFLOW_HOME})

# # Create connection object
session = settings.Session()
session.add(conn_news_api)
session.add(conn_filesystem)
session.commit()


# DAG Object
dag = DAG(
    'tempus_bonus_challenge_dag',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
)

# define workflow tasks
# begin workflow
start_task = DummyOperator(task_id='start', retries=3, dag=dag)

# create a folder for storing retrieved data on the local filesystem
datastore_creation_task = PythonOperator(
    task_id='create_storage_task',
    provide_context=True,
    python_callable=c.FileStorage.create_storage,
    dag=dag
)

# retrieve all news based on keywords
# Need to make four SimpleHTTPOperator calls run in parallel
news_kw1_task = SimpleHttpOperator(endpoint='v2/everything?',
                                   method='GET',
                                   data={'q': 'Tempus Labs',
                                         'apiKey': API_KEY},
                                   response_check=c.NetworkOperations.get_news,
                                   http_conn_id='newsapi',
                                   task_id='get_news_first_kw_task',
                                   retries=3,
                                   dag=dag)

news_kw2_task = SimpleHttpOperator(endpoint='v2/everything?',
                                   method='GET',
                                   data={'q': 'Eric Lefkofsky',
                                         'apiKey': API_KEY},
                                   response_check=c.NetworkOperations.get_news,
                                   http_conn_id='newsapi',
                                   task_id='get_news_second_kw_task',
                                   retries=3,
                                   dag=dag)

news_kw3_task = SimpleHttpOperator(endpoint='v2/everything?',
                                   method='GET',
                                   data={'q': 'Cancer',
                                         'apiKey': API_KEY},
                                   response_check=c.NetworkOperations.get_news,
                                   http_conn_id='newsapi',
                                   task_id='get_news_third_kw_task',
                                   retries=3,
                                   dag=dag)

news_kw4_task = SimpleHttpOperator(endpoint='v2/everything?',
                                   method='GET',
                                   data={'q': 'Immunotherapy',
                                         'apiKey': API_KEY},
                                   response_check=c.NetworkOperations.get_news,
                                   http_conn_id='newsapi',
                                   task_id='get_news_fourth_kw_task',
                                   retries=3,
                                   dag=dag)

# detect existence of retrieved data
file_exists_sensor = DummyOperator(task_id='file_sensor', retries=3, dag=dag)

# retrieve all of the top headlines
retrieve_headlines_task = DummyOperator(task_id='get_headl_kw_task', dag=dag)

# transform the data, resulting in a flattened csv
flatten_csv_task = DummyOperator(task_id='transform_kw_task', dag=dag)

# upload the flattened csv into my S3 bucket
upload_csv_task = DummyOperator(task_id='upload_kw_task', dag=dag)

# end workflow
end_task = DummyOperator(task_id='end', dag=dag)


# arrange the workflow tasks
# create folder that acts as 'staging area' to store retrieved
# data before processing. In a production system this would be
# a real database.
start_task >> datastore_creation_task >> news_kw1_task

# ensure the data has been retrieved before beginning the ETL process.
news_kw1_task >> file_exists_sensor

# all the news sources are retrieved, the top headlines
# extracted, and the data transform by flattening into CSV.
file_exists_sensor >> retrieve_headlines_task >> flatten_csv_task

# perform a file transfer operation, uploading the CSV data
# into S3 from local.
flatten_csv_task >> upload_csv_task >> end_task
