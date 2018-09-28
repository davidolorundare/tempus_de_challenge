"""Tempus challenge  - Airflow data pipeline definition.

Describes a data pipeline that would fetch data from the News API, transform it
into a tabular structure, and finally stored the transformation in an Amazon S3
Bucket.
"""

from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 28),
    'email': ['david.o@ieee.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG Object
dag = DAG(
    'tempus_challenge_dag',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
)


# retrieve all english news sources
retrieve_news_task = DummyOperator(task_id='get_news_task', dag=dag)

# retrieve all of the top headlines
retrieve_headlines_task = DummyOperator(task_id='get_headlines_task', dag=dag)

# transform the data, resulting in a flattened csv
flatten_csv_task = DummyOperator(task_id='transform_task', dag=dag)

# upload the flattened csv into my S3 bucket
upload_csv_task = DummyOperator(task_id='upload_task', dag=dag)

# end workflow
dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

# arrange the workflow tasks
# all the news sources are retrieved, the top headlines
# extracted, and the data transform by flattening into CSV.
retrieve_news_task >> retrieve_headlines_task >> flatten_csv_task

# perform a file transfer operation, uploading the CSV data
# into S3 from local.
flatten_csv_task >> upload_csv_task >> dummy_task
