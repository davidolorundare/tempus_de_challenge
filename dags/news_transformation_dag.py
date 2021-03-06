"""Tempus challenge  - Airflow data pipeline definition.

Describes a data pipeline that would fetch data from the News API, transform it
into a tabular structure, and finally stored the transformation in an Amazon S3
Bucket.

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
NEWS_DIRECTORY = "usr/local/airflow/tempdata/tempus_challenge_dag/news/"

# MAINTAIN SECRECY OF API KEYS
# https://12factor.net/config
# https://devops.stackexchange.com/questions/3902/passing-secrets-to-a-docker-container
# https://docs.aws.amazon.com/general/latest/gr/aws-access-keys-best-practices.html
# https://medium.com/@mccode/dont-embed-configuration-or-secrets-in-docker-images-7b2e0f916fdd
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
dag = DAG('tempus_challenge_dag',
          default_args=default_args,
          schedule_interval='0 0 * * *',
          catchup=False)


# define workflow tasks
# begin workflow
start_task = DummyOperator(task_id='start', dag=dag)


# use an alias since the length of the real function call is more than
# PEP-8's 79 line-character limit.
storage_func_alias = FileStorage.create_storage
news_func_alias = NetworkOperations.get_news
headlines_func_alias = NetworkOperations.get_news_headlines
transform_func_alias = TransformOperations.transform_headlines_to_csv
upload_func_alias = UploadOperations.upload_csv_to_s3

# creates a folder for storing retrieved data on the local filesystem
datastore_creation_task = PythonOperator(task_id='create_storage_task',
                                         provide_context=True,
                                         python_callable=storage_func_alias,
                                         retries=3,
                                         dag=dag)

# retrieve all english news sources
# Using the News API, a http request is made to the News API's 'sources'
# endpoint, with its 'language' parameter set to 'en'.
get_news_task = SimpleHttpOperator(endpoint='/v2/sources?',
                                   method='GET',
                                   data={'language': 'en',
                                         'apiKey': API_KEY},
                                   response_check=news_func_alias,
                                   http_conn_id='newsapi',
                                   task_id='get_news_sources_task',
                                   dag=dag,
                                   depends_on_past=True,
                                   retry_delay=timedelta(minutes=3),
                                   retry_exponential_backoff=True)

# detect existence of retrieved news data
file_exists_sensor = FileSensor(filepath=NEWS_DIRECTORY,
                                fs_conn_id="filesys",
                                poke_interval=5,
                                soft_fail=True,
                                timeout=3600,
                                task_id='file_sensor_task',
                                dag=dag)

# retrieve each sources headlines and perform subsequent
# headline-extraction step
headlines_task = PythonOperator(task_id='extract_headlines_task',
                                provide_context=True,
                                python_callable=headlines_func_alias,
                                retries=3,
                                dag=dag)

# extract and transform the data, resulting in a flattened csv
flatten_csv_task = PythonOperator(task_id='flatten_to_csv_task',
                                  provide_context=True,
                                  python_callable=transform_func_alias,
                                  retries=3,
                                  dag=dag)

# upload the flattened csv into my S3 bucket
upload_csv_task = PythonOperator(task_id='upload_csv_to_s3_task',
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
start_task >> datastore_creation_task >> get_news_task >> file_exists_sensor

# ensure the data has been retrieved before beginning the ETL process.
# all the news sources are retrieved, the top headlines extracted,
# and the data transform by flattening into CSV.
file_exists_sensor >> headlines_task >> flatten_csv_task

# perform a file transfer operation, uploading the CSV data
# into S3 from local.
flatten_csv_task >> upload_csv_task >> end_task
