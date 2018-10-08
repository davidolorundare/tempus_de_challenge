"""Tempus challenge  - Airflow data pipeline definition.

Describes a data pipeline that would fetch data from the News API, transform it
into a tabular structure, and finally stored the transformation in an Amazon S3
Bucket.
"""

from datetime import datetime, timedelta


from airflow import DAG
from airflow import settings
from airflow.models import Connection
# from airflow.models import Variable
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

# Connection objects for the News API endpoints
conn_news_api = Connection(conn_id="newsapi",
                           conn_type="HTTP",
                           host="https://newsapi.org")

# # Create connection object
session = settings.Session()
session.add(conn_news_api)
session.commit()


# DAG Object
dag = DAG(
    'tempus_challenge_dag',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
)


# define workflow tasks
# begin workflow
start_task = DummyOperator(task_id='start', dag=dag)

# creates a folder for storing retrieved data on the local filesystem
# datastore_creation_task = PythonOperator(
#     task_id='create_storage_task',
#     provide_context=True,
#     python_callable=c.FileStorage.create_storage,
#     dag=dag
# )

# NEED TO MAINTAIN SECRECY OF API KEYS
# https://12factor.net/config
# retrieve all english news sources
# Using the News API, a http request is made to the News API's 'sources'
# endpoint, with its 'language' parameter set to 'en'.
get_news_task = SimpleHttpOperator(endpoint='/v2/sources?',
                                   method='GET',
                                   data={'language': 'en',
                                         'apiKey':
                                         '68ce2435405b42e5b4a90080249c6962'},
                                   response_check=c.NetworkOperations.get_news,
                                   http_conn_id='newsapi',
                                   task_id='get_news_sources_task',
                                   dag=dag)

# detect existence of retrieved data
# file_exists_sensor = DummyOperator(task_id='file_sensor', retries=3, dag=dag)

# retrieve all of the top headlines
# retrieve_headlines_task = DummyOperator(task_id='get_headlines_task',
#                                         retries=3,
#                                         dag=dag)

# transform the data, resulting in a flattened csv
# flatten_csv_task = DummyOperator(task_id='transform_task', retries=3,dag=dag)

# upload the flattened csv into my S3 bucket
# upload_csv_task = DummyOperator(task_id='upload_task', retries=3, dag=dag)

# end workflow
end_task = DummyOperator(task_id='end', dag=dag)


# arrange the workflow tasks
# create folder that acts as 'staging area' to store retrieved
# data before processing. In a production system this would be
# a real database.
# start_task >> datastore_creation_task >> get_news_task

# ensure the data has been retrieved before beginning the ETL process.
# get_news_task >> file_exists_sensor

# all the news sources are retrieved, the top headlines
# extracted, and the data transform by flattening into CSV.
# file_exists_sensor >> retrieve_headlines_task >> flatten_csv_task

# perform a file transfer operation, uploading the CSV data
# into S3 from local.
# flatten_csv_task >> upload_csv_task >> end_task


start_task >> get_news_task >> end_task
