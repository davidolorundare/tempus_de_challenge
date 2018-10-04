"""Tempus Bonus challenge - Airflow data pipeline definition.

Describes a data pipeline that would fetch data from the News API based on
four keywords - 'Tempus Labs', 'Eric Lefkofsky', 'Cancer', and 'Immunotherapy'.
The data is transformed into a tabular structure, and finally stored the an AWS
S3 Bucket.
"""

from datetime import datetime, timedelta


from airflow import DAG
#from airflow.models import Connection
from airflow.operators.dummy_operator import DummyOperator


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
# conn_news_sources = Connection(
#         conn_id="newsapi_sources",
#         conn_type="HTTP",
#         host="https://newsapi.org/v2/sources?"
# )

# conn_news_everything = Connection(
#         conn_id="newsapi_everything",
#         conn_type="HTTP",
#         host="https://newsapi.org/v2/everything?"
# )

# conn_news_headlines = Connection(
#         conn_id="newsapi_headlines",
#         conn_type="HTTP",
#         host="https://newsapi.org/v2/top-headlines?"
# )


# # Create connection object
# session = settings.Session()
# session.add(conn_news_sources)
# session.add(conn_news_everything)
# session.add(conn_news_headlines)
# session.commit()


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

# create a folder for storing retrieved data on local filesystem
# is a PythonOperator
datastore_creation_task = DummyOperator(task_id='create_storage_task', dag=dag)

# retrieve all news based on keywords
retrieve_news_kw_task = DummyOperator(task_id='get_news_kw_task', dag=dag)

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
start_task >> datastore_creation_task >> retrieve_news_kw_task

# ensure the data has been retrieved before beginning the ETL process.
retrieve_news_kw_task >> file_exists_sensor

# all the news sources are retrieved, the top headlines
# extracted, and the data transform by flattening into CSV.
file_exists_sensor >> retrieve_headlines_task >> flatten_csv_task

# perform a file transfer operation, uploading the CSV data
# into S3 from local.
flatten_csv_task >> upload_csv_task >> end_task
