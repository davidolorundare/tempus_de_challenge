"""Tempus Bonus challenge - Airflow data pipeline definition.

Describes a data pipeline that would fetch data from the News API based on
four keywords - 'Tempus Labs', 'Eric Lefkofsky', 'Cancer', and 'Immunotherapy'.
The data is transformed into a tabular structure, and finally stored in an AWS
S3 Bucket.

If for any reason a task that is being run fails, they have been configured to
try to re-run it after a time delay. This behaviour is helpful in case systems
are temporarily unavailable (e.g. the News API server). The number of retries
are configured at both DAG-level and at Task-level. Once all the possible runs
have been exhausted and the system continuously failed, the task will be marked
as failed.
"""

import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow import settings
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Connection
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator

from challenge.network.network_operations import NetworkOperations
from challenge.transform.transform_operations import TransformOperations
from challenge.upload.upload_operations import UploadOperations
from challenge.storage.filestorage_operations import FileStorage


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 22),
    'email': ['david.o@ieee.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}


# path, relative to AIRFLOW_HOME, to the news folder which
# stores data from the News API
NEWS_DIR = "usr/local/airflow/tempdata/tempus_bonus_challenge_dag/headlines/"

# MAINTAIN SECRECY OF API KEYS
# https://12factor.net/config
# https://docs.aws.amazon.com/general/latest/gr/aws-access-keys-best-practices.html
# this should NEVER be hardcoded (at least put it in an environment variable)
# See project README for more details.
API_KEY = os.environ["NEWS_API_KEY"]

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
storage_func_alias = FileStorage.create_storage
headlines_func_alias = NetworkOperations.get_news_keyword_headlines
flatten_csv_func_alias = TransformOperations.transform_headlines_to_csv
upload_func_alias = UploadOperations.upload_csv_to_s3

# create a folder for storing retrieved data on the local filesystem
datastore_creation_task = PythonOperator(task_id='create_storage_task',
                                         provide_context=True,
                                         python_callable=storage_func_alias,
                                         retries=3,
                                         dag=dag)

# # retrieve all top news headlines for specific keywords
# # Need to make four SimpleHTTPOperator calls run in parallel
news_kw1_task = SimpleHttpOperator(endpoint='/v2/top-headlines?',
                                   method='GET',
                                   data={'q': 'Tempus Labs',
                                         'apiKey': API_KEY},
                                   response_check=headlines_func_alias,
                                   http_conn_id='newsapi',
                                   task_id='get_headlines_first_kw_task',
                                   dag=dag,
                                   retry_delay=timedelta(minutes=3),
                                   retry_exponential_backoff=True,
                                   depends_on_past=True)

news_kw2_task = SimpleHttpOperator(endpoint='/v2/top-headlines?',
                                   method='GET',
                                   data={'q': 'Eric Lefkofsky',
                                         'apiKey': API_KEY},
                                   response_check=headlines_func_alias,
                                   http_conn_id='newsapi',
                                   task_id='get_headlines_second_kw_task',
                                   dag=dag,
                                   retry_delay=timedelta(minutes=3),
                                   retry_exponential_backoff=True,
                                   depends_on_past=True)

news_kw3_task = SimpleHttpOperator(endpoint='/v2/top-headlines?',
                                   method='GET',
                                   data={'q': 'Cancer',
                                         'apiKey': API_KEY},
                                   response_check=headlines_func_alias,
                                   http_conn_id='newsapi',
                                   task_id='get_headlines_third_kw_task',
                                   dag=dag,
                                   retry_delay=timedelta(minutes=3),
                                   retry_exponential_backoff=True,
                                   depends_on_past=True)

news_kw4_task = SimpleHttpOperator(endpoint='/v2/top-headlines?',
                                   method='GET',
                                   data={'q': 'Immunotherapy',
                                         'apiKey': API_KEY},
                                   response_check=headlines_func_alias,
                                   http_conn_id='newsapi',
                                   task_id='get_headlines_fourth_kw_task',
                                   dag=dag,
                                   retry_delay=timedelta(minutes=3),
                                   retry_exponential_backoff=True,
                                   depends_on_past=True)

# # detect existence of retrieved news data
file_exists_sensor = FileSensor(filepath=NEWS_DIR,
                                fs_conn_id="filesys",
                                poke_interval=5,
                                soft_fail=True,
                                timeout=3600,
                                task_id='file_sensor_task',
                                dag=dag)

# extract and transform the data, resulting in a flattened csv
flatten_to_csv_task = PythonOperator(task_id='flatten_to_csv_kw_task',
                                     provide_context=True,
                                     python_callable=flatten_csv_func_alias,
                                     retries=3,
                                     dag=dag,
                                     depends_on_past=True)

# # upload the flattened csv into my S3 bucket
upload_csv_task = PythonOperator(task_id='upload_csv_to_s3_kw_task',
                                 provide_context=True,
                                 python_callable=upload_func_alias,
                                 retries=3,
                                 dag=dag,
                                 depends_on_past=True)

# # end workflow
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
# Then perform a file transfer operation, uploading the CSV data
# into S3 from local.
file_exists_sensor >> flatten_to_csv_task >> upload_csv_task >> end_task
