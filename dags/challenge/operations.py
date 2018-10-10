"""Tempus challenge  - Operations and Functions.

Describes the code definition of the Airflow Operator for the tasks in the DAG.
The 'Tempus Bonus Challenge' dag performs similar tasks to those of the
'Tempus Challenge' dag. Hence, to encourage function reusability, all the
functions executed by both dag pipelines are implemented in the same Operations
class.
"""

import json
import logging
import os
import time

# from airflow.models import Variable

import requests


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
            context: current Airflow context in which the function/operator
                is being run in.
        """
        log.info("Running create_storage method")

        # list of the directories that will be created to store data
        data_directories = ['news', 'headlines', 'csv']
        log.info("Running create_storage method")
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
            path_join_func: function to use for creating the directory path for
                the datastore directories. Default is Python's os.path.join()
            dir_func: function to use for making the actual datastore folders.
                Default is Python's os.makedirs() function.
            context: the current Airflow context in which the function/operator
                is being run in.
        """
        log.info("Running create_data_stores method")

        # stores the dag_id which will be the name of the created folder
        dag_id = str(context['dag'].dag_id)
        # Push the dag_id to the downstream SimpleHTTPOperator task
        # Using:
        # Variable.set("current_dag_id", dag_id)
        # gives error when testing locally.
        # Switch to using Python Environ variables
        os.environ["current_dag_id"] = dag_id

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

        if os.path.isdir(dir_path):
            return True
        else:
            return False

    @classmethod
    def write_json_to_file(cls,
                           data,
                           path_to_dir,
                           create_date=None,
                           filename=None):
        """write given json news data to an existing directory.


        # Arguments
            create_date: date the file was created.
            data: the json string data to be written to file.
            path_to_dir: folder path where the json file will be stored in.
            filename: the name of the created json file.

        Checks if the json data and directory are valid, otherwise raises
        error exceptions. the files are prefixed with the current datetime.
        """
        log.info("Running write_json_to_file method")

        if not os.path.isdir(path_to_dir):
            raise OSError("Directory {} does not exist".format(path_to_dir))
        if not create_date:
            create_date = time.strftime("%Y%m%d-%H%M%S")
        if not filename:
            filename = "sample"

        # validate the input json string data
        try:
            json.loads(data)
        except ValueError:
            raise ValueError("Error Decoding - Data is not valid JSON")

        # create the filename and its extension, append date
        fname = str(create_date) + "_" + str(filename) + ".json"
        fpath = os.path.join(path_to_dir, fname)
        # write the json string data to file.
        try:
            with open(fpath, 'w') as outputfile:
                json.dump(data, outputfile)
            return True
        except IOError:
            raise IOError("Error in Reading Data - IOError")

    @classmethod
    def get_news_directory(cls, pipeline_name: str):
        """Return the news directory path for a given DAG pipeline.

        For production code this function would be refactored to read-in
        the directory structure from an external config file.

        # Arguments:
            pipeline_name: the name or ID of the current DAG pipeline
                running this script.
        """

        # mapping of the dag_id to the appropriate 'news' folder
        log.info("Running get_news_directory method")

        news_path = os.path.join(HOME_DIRECTORY,
                                 'tempdata',
                                 'tempus_challenge_dag',
                                 'news')

        news_bonus_path = os.path.join(HOME_DIRECTORY,
                                       'tempdata',
                                       'tempus_bonus_challenge_dag',
                                       'news')

        news_store = {'tempus_challenge_dag': news_path,
                      'tempus_bonus_challenge_dag': news_bonus_path}

        if pipeline_name not in news_store:
            raise ValueError("No directory path for given pipeline name")

        return news_store[pipeline_name]

    @classmethod
    def get_headlines_directory(cls, pipeline_name: str):
        """Return the headlines directory path for a given DAG pipeline.

        For production code this function would be refactored to read-in
        the directory structure from an external config file.

        # Arguments:
            pipeline_name: the name or ID of the current DAG pipeline
                running this script.
        """

        # mapping of the dag_id to the appropriate 'headlines' folder
        log.info("Running get_headlines_directory method")

        headlines_path = os.path.join(HOME_DIRECTORY,
                                      'tempdata',
                                      'tempus_challenge_dag',
                                      'headlines')

        headlines_bonus_path = os.path.join(HOME_DIRECTORY,
                                            'tempdata',
                                            'tempus_bonus_challenge_dag',
                                            'headlines')

        headlines_store = {'tempus_challenge_dag': headlines_path,
                           'tempus_bonus_challenge_dag': headlines_bonus_path}

        if pipeline_name not in headlines_store:
            raise ValueError("No directory path for given pipeline name")

        return headlines_store[pipeline_name]

    @classmethod
    def get_csv_directory(cls, pipeline_name: str):
        """Return the csv directory path for a given DAG pipeline.

        For production code this function would be refactored to read-in
        the directory structure from an external config file.
        """

        # mapping of the dag_id to the appropriate 'csv' folder
        log.info("Running get_csv_directory method")

        csv_path = os.path.join(HOME_DIRECTORY,
                                'tempdata',
                                'tempus_challenge_dag',
                                'csv')
        csv_bonus_path = os.path.join(HOME_DIRECTORY,
                                      'tempdata',
                                      'tempus_bonus_challenge_dag',
                                      'csv')
        csv_store = {'tempus_challenge_dag': csv_path,
                     'tempus_bonus_challenge_dag': csv_bonus_path}

        if pipeline_name not in csv_store:
            raise ValueError("No directory path for given pipeline name")

        return csv_store[pipeline_name]


class NetworkOperations:
    """Handles functionality making remote calls to the News API."""

    @classmethod
    def get_news_data(cls, response: requests.Response):
        """Processes the response from the API call to get all english news sources.


        Returns True is the response is valid and stores the content in the
        folder appropriately. Returns False if the response is invalid.
        The function also needs to return True for the SimpleHTTPOperator
        response_check parameter to 'pass' or False to indicate its failure

        On successful resposne the json content of the response is store in the
        appropriate 'news' datastore folder based on dag_id context
        (need to determine this).

        # Arguments
            response: HTTP Response object returned from the SimpleHTTPOperator
                http call.

        """
        log.info("Running get_news_data method")
        # check the status code, if is is valid OK then save the result into
        # the appropriate news directory.
        status_code = response.status_code
        log.info(status_code)

        # retrieve the context-specific news directory path from upstream task
        pipeline_name = os.environ.get("current_dag_id")
        news_dir_path = FileStorage.get_news_directory(pipeline_name)

        # copy of the json string data
        json_data = response.json()

        # write the data to file
        if status_code == requests.codes.ok:
            FileStorage.write_json_to_file(json_data,
                                           news_dir_path,
                                           filename="english_news_sources")
            return [True, status_code]
        elif status_code >= 400:
            return [False, status_code]
        else:
            return [False, status_code]

    @classmethod
    def get_headlines(cls):
        """Process the response from the API call to get headlines"""


class ExtractOperations:
    """Handles functionality for extracting headlines.

    - error handling
    - parsing json
    """


class TransformOperations:
    """Handles functionality for flattening CSVs"""


class UploadOperations:
    """Handles functionality for uploading flattened CSVs"""


def process_retrieved_data(self):
    """For each news performs a series of ETL operations"""
