"""Tempus challenge  - Operations and Functions.

Describes the code definition of the PythonOperator tasks in the DAG.
The 'Tempus Bonus Challenge' dag performs similar tasks to those of the
'Tempus Challenge' dag. Hence, to encourage function reusability, all the
functions executed by both dag pipelines are implemented in the same Operations
class.
Network Call to get News, Extract Headlines, Flatten CSV, Upload CSV
"""

import errno
import json
import logging
import os

import requests


# from airflow.models import Variable

log = logging.getLogger(__name__)

# store the current directory of the airflow home folder
# airflow creates a home environment variable pointing to the location
HOME_DIRECTORY = str(os.environ['HOME'])


class FileStorage:
    """Handles functionality for data storage"""

    @classmethod
    def create_storage(cls, **context):
        """Create tempoary data storage for the current DAG pipeline.

        # Arguments
            context: the current Airflow context in which the function/operator
                is being run in.
        """

        # list of the directories that will be created to store data
        data_directories = ['news', 'headlines', 'csv']

        for name in data_directories:
            cls.create_data_stores(dir_name=name, **context)

    @classmethod
    def create_data_stores(cls,
                           dir_name,
                           path_join_func=os.path.join,
                           dir_func=os.makedirs,
                           **context):
        """Create a set of datastore folders in the local filesystem.


        Creates a 'data' folder in the AIRFLOW_HOME directory, if it doesn't
        already exist (otherwise it replaces the existing one), in which to
        temporaily store the JSON data retrieved from the News API for further
        processing downstream.
        Using the name of the pipeline e.g. 'tempus_challenge' or
        'tempus_bonus_challenge' from the passed in context and creates the
        appropriate subdirectories for storing the intermediary data - the
        extracted top-headlines and converted csv, before the transformed data
        is uploaded to its final destination.


        # Arguments
            dir_name: the name of the datastore directory to create.
            path_join_func: the function to use for creating the directory path
                for the datastore directories. Default is Python's os.path.join
            dir_func: the function to use for making the actual datastore
                directories. Default is Python's os.makedirs
            context: the current Airflow context in which the function/operator
                is being run in.
        """

        # stores the dag_id which will be the name of the created folder
        dag_id = str(context['dag'].dag_id)

        # create a data folder and subdirectories for the dag
        # if the data folder doesnt exist, create it and the subdirs
        # if it exists, create the subdirs
        try:
            dir_path = path_join_func(HOME_DIRECTORY,
                                      'tempdata',
                                      dag_id,
                                      dir_name)
            dir_func(dir_path, exist_ok=True)
        except IOError as err:
            print("I/O error({0}): {1}".format(err.errno, err.strerror))


class NetworkOperations:
    """Handles functionality for news retrieval."""

    def retrieve_english_news(self, url):
        """Returns all english news sources.

        Using the News API, a http request is made to the
        News API's 'sources' endpoint, with its 'language'
        parameter set to 'en'.
        A json object is returned containing all retrieved
        English news sources.
        Note APIKey from Variables.
        - storing apikey
        - error handling
        - parsing json
        """

        # response = requests.get(url)

        return "all news"


class ExtractOperations:
    """Handles functionality for extracting headlines"""


class TransformOperations:
    """Handles functionality for flattening CSVs"""


class UploadOperations:
    """Handles functionality for uploading flattened CSVs"""


def process_retrieved_data(self):
    """For each news performs a series of ETL operations"""
