"""Simple DAG that uses a few python operators."""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import challenge as c

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 4, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Object
dag = DAG(
    'sample_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),  # DAG will run once every 5 minutes
    catchup=False,
)

print_context_task = PythonOperator(
    task_id='print_context',
    provide_context=True,
    # provide params and additional kwargs to python_callable
    python_callable=c.print_context,
    dag=dag
)

print_date_task = PythonOperator(
    task_id='print_date',
    provide_context=True,  # necessary to provide date to python_callable
    python_callable=c.PrintExecutionDate.callable,
    dag=dag
)

hello_world_task = PythonOperator(
    task_id='hello_world',
    provide_context=True,  # necessary to provide params to python_callable
    python_callable=c.HelloWorld(),
    params={'name': 'Data Engineer'},
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# A visual representation of the following should be viewable at:
# http://localhost:8080/admin/airflow/graph?dag_id=sample_dag
# >> and << operators sets upstream and downstream relationships
# print_date_task is downstream from print_context_task.
# In other words, print_date_task will run after print_context_task
print_context_task >> print_date_task
# print_date_task is upstream from end
# In other words, print_date_task will run before end
end << print_date_task
print_context_task >> hello_world_task >> end
