
import errno
import json
import logging
import os
import requests
import shutil
import time

from airflow.models import Variable

from dags import challenge as c

log = logging.getLogger(__name__)

# store the current directory of the airflow home folder
# airflow creates a home environment variable pointing to the location
HOME_DIRECTORY = str(os.environ['HOME'])


class FileStorage:
    """Handles functionality for news data storage on the local filesystem."""

    @classmethod
    def dummy_function(cls, dummy_arg=None):
        """Function that does absolutely nothing.

        But, is useful in some of the code below.

        # Arguments:
            :param dummy_arg: some dummy argument.
            :type dummy_arg: can be anything. Default is None.
        """
        pass

    @classmethod
    def create_storage(cls, **context):
        """Create tempoary data storage for the current DAG pipeline.

        # Arguments
            :param context: current Airflow context in which the function or
                operator is being run in.
            :type context: dict
        """

        log.info("Running create_storage method")

        # stores the dag_id which will be the name of the created folder
        dag_id = str(context['dag'].dag_id)

        # Python's os.environ property is used, in its place, during unit
        # tests for setting environment variables. However, the variables
        # don't seem to carry over between Airflow tasks. (Using either
        # Airflow's Variable or XCom classes might be more ideal here.)
        os.environ["current_dag_id"] = dag_id

        # Push the dag_id to the downstream SimpleHTTPOperator task
        # Using Airflow's global Variables:
        Variable.set("current_dag_id", dag_id)

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
            :param dir_name: the name of the datastore directory to create.
            :type dir_name: str
            :param path_join_func: function to use for creating the directory
                path for the datastore directories. Default is Python's
                os.path.join()
            :type path_join_func: str
            :param dir_func: function to use for making the actual datastore
                folders. Default is Python's os.makedirs() function.
            :type dir_func: function
            :param context: the current Airflow context in which the function
                or operator is being run in.
            :type context: dict

        # Raises:
            OSError: if the directory path given does not exist.
        """

        log.info("Running create_data_stores method")

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
            # idempotency - if those news,headlines,csv folders
            # already exist then delete them before starting the
            # a fresh pipeline run.
            if os.path.exists(dir_path) and os.path.isdir(dir_path):
                shutil.rmtree(dir_path)
            dir_func(dir_path, exist_ok=True)
        # using exist_ok=True in makedirs would still raise FileExistsError
        # if target path exists and it is not a directory (e.g. file,
        # block device)
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise

        # return True if the directory was created, otherwise False.
        if os.path.isdir(dir_path):
            # airflow logging
            log.info("Created Directory: {}".format(dir_path))

            return True
        else:
            return False

    @classmethod
    def write_json_to_file(cls,
                           data,
                           path_to_dir,
                           filename=None,
                           create_date=None):
        """writes given json news data to an existing directory.

        Perfoms checks if the json data and directory are valid, otherwise
        raises error exceptions. the files are prefixed with the current
        datetime.

        # Arguments

            :param data: the json data to be written to file.
            :type data: dict
            :param path_to_dir: folder path where the json file will be
                stored in.
            :type path_to_dir: str
            :param filename: the name of the crejsoated json file.
            :type filename: str
            :param create_date: date the file was created.
            :type create_date: str

        # Raises:
            OSError: if the directory path given does not exist.
            ValueError: if it fails to validate the input json data.
            IOError: if it fails to write the validated json data as a file
                to the given directory.
        """

        log.info("Running write_json_to_file method")

        if not os.path.isdir(path_to_dir):
            raise OSError("Directory {} does not exist".format(path_to_dir))
        if not create_date:
            create_date = time.strftime("%Y%m%d-%H%M%S")
        if not filename:
            filename = "sample"

        # json validation
        try:
            # validate the input json string data
            validated_data = json.loads(json.dumps(data))

            # we do not really need the variable 'validated_data'
            # but leaving it as just json.loads() will inadvertently
            # end up dumpping the large parsed json data to the logs,
            # which is NOT what we want.

            # to satisfy PEP-8 requirement that declared variables
            # should not be unused. let's use it to print something useful.
            # print("data is valid json of \
            #    type {}".format(type(validated_data)))

            # reset validated_data to None, since we don't really use it
            # outside of logging, and having many print  statements of this
            # same function running will just fill up the logs quickly.
            cls.dummy_function(validated_data)
        except ValueError:
            raise ValueError("Error Decoding - Data is not Valid JSON")

        # create the filename and its extension, append date
        fname = str(create_date) + "_" + str(filename) + ".json"
        fpath = os.path.join(path_to_dir, fname)

        # write the json string data to file.
        try:
            with open(fpath, 'w+') as outputfile:
                json.dump(data, outputfile)
            return True
        except IOError:
            raise IOError("Error in Reading Data - IOError")

    @classmethod
    def write_source_headlines_to_file(cls,
                                       source_ids,
                                       source_names,
                                       headline_dir,
                                       api_key,
                                       headline_func=None):
        """writes extracted news source headline json data to an existing directory.

        # Arguments:
            :param source_ids: list of news source id tags
            :type source_ids: list
            :param source_names: list of news source name tags
            :type source_names: list
            :param headline_dir: directory path in which the source-headlines
                should be stored in.
            :type headline_dir: str
            :param api_key: string News API Key used for performing retrieval
                of a source's top headlines remotely
            :type api_key: str
            :param headline_func: function to use for extracting headlines.
            :type headline_func: function

        # Raises:
            ValueError: if any of the arguments are left blank
        """

        log.info("Running write_source_headlines_to_file method")

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        if not headline_func:
            headline_func = c.NetworkOperations.get_source_headlines

        # error check for non-set arguments
        if not source_ids:
            raise ValueError("Argument '{}' is blank".format(source_ids))
        if not source_names:
            raise ValueError("Argument '{}' is blank".format(source_names))
        if not headline_dir:
            raise ValueError("Argument '{}' is blank".format(headline_dir))
        if not api_key:
            raise ValueError("Argument '{}' is blank".format(api_key))

        # get the headlines of each source
        for index, value in enumerate(source_ids):
            headlines_obj = headline_func(value, api_key=api_key)
            if headlines_obj.status_code == requests.codes.ok:
                headline_json = headlines_obj.json()
                # descriptive name of the headline file.
                # use the source id rather than source name, since
                # (after testing) it was discovered that strange formattings
                # like 'Reddit /r/all' get read by the open() like a directory
                # path rather than a filename, and hence requires another
                # separate parsing all together.
                # Is of the form  'source_id' + '_headlines'
                fname = str(value) + "_headlines"

                # write this json object to the headlines directory
                c.FileStorage.write_json_to_file(headline_json,
                                                 headline_dir,
                                                 fname)

        # return with a verification that these operations succeeded
        if os.listdir(headline_dir):
            # airflow logging
            log.info("Files in Headlines Directory: ")
            log.info(os.listdir(headline_dir))

            return True
        else:
            return False

    @classmethod
    def get_news_directory(cls, pipeline_name: str):
        """returns the news directory path for a given DAG pipeline.

        For production code this function would be refactored to read-in
        the directory structure from an external config file.

        # Arguments:
            :param pipeline_name: the name or ID of the current DAG pipeline
                running this script.
            :type pipeline_name: str

        # Raises:
            ValueError: if the given pipeline name does not exist in a
            predefined list mapping of pipline names to their respective
            directories.
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
        """returns the headlines directory path for a given DAG pipeline.

        For production code this function would be refactored to read-in
        the directory structure from an external config file.

        # Arguments:
            :param pipeline_name: the name or ID of the current DAG pipeline
                running this script.
            :type pipeline_name: str

        # Raises:
            ValueError: if the given pipeline name does not exist in a
                predefined list mapping of pipline names to their respective
                directories.
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
        """returns the csv directory path for a given DAG pipeline.

        For production code this function would be refactored to read-in
        the directory structure from an external config file.

        # Arguments:
            :param pipeline_name: the name or ID of the current DAG pipeline
                running this script.
            :type pipeline_name: str

        # Raises:
            ValueError: if the given pipeline name does not exist in a
            predefined list mapping of pipline names to their respective
            directories.
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
