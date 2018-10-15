"""Tempus challenge  - Operations and Functions.

Describes the code definition of the Airflow Operator for the tasks in the DAG.
The 'Tempus Bonus Challenge' dag performs similar tasks to those of the
'Tempus Challenge' dag. Hence, to encourage function reusability, all the
functions executed by both dag pipelines are implemented in the same Operations
class.
"""

import config
import datetime
import errno
import gc
import numpy as np
import json
import logging
import os
import pandas as pd
import shutil
import time

from airflow.hooks import S3_hook
from airflow.models import Variable

import requests


log = logging.getLogger(__name__)

# store the current directory of the airflow home folder
# airflow creates a home environment variable pointing to the location
HOME_DIRECTORY = str(os.environ['HOME'])


class MissingApiKeyError(ValueError):
    """raised when no api key is found or set."""
    pass


class NoFilesFoundError(FileNotFoundError):
    """raised no files of a particular type exist in a directory"""
    pass


class FileStorage:
    """Handles functionality for data storage."""

    @classmethod
    def dummy_function(cls, dummy_arg=None):
        """function that does absolutely nothing.

        But, was found useful in some of the code below.

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
            :type dir_name: string
            :param path_join_func: function to use for creating the directory
                path for the datastore directories. Default is Python's
                os.path.join()
            :type path_join_func: string
            :param dir_func: function to use for making the actual datastore
                folders. Default is Python's os.makedirs() function.
            :type dir_func: string
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
            :type path_to_dir: string
            :param filename: the name of the crejsoated json file.
            :type filename: string
            :param create_date: date the file was created.
            :type create_date: string

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
            :type headline_dir: string
            :param api_key: string News API Key used for performing retrieval
                of a source's top headlines remotely
            :type api_key: string
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
            headline_func = NetworkOperations.get_source_headlines

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
                FileStorage.write_json_to_file(headline_json,
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
            :type pipeline_name: string

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
            :type pipeline_name: string

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
            :type pipeline_name: string

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


class NetworkOperations:
    """handles functionality making remote calls to the News API."""

    @classmethod
    def get_news(cls,
                 response: requests.Response,
                 news_dir=None,
                 filename=None,
                 gb_var=None):
        """processes the response from the API call to get all english news sources.

        Returns True is the response is valid and stores the content in the
        folder appropriately. Returns False if the response is invalid.
        The function also needs to return True for the SimpleHTTPOperator
        response_check parameter to 'pass' or False to indicate its failure

        On successful resposne the json content of the response is store in the
        appropriate 'news' datastore folder based on dag_id context
        (need to determine this).

        # Arguments
            :param response: http response object returned from the
                SimpleHTTPOperator http call.
            :type response: Response object
            :param news_dir: directory to store the news data to.
            :type news_dir: string
            :param filename: name of the json file created from the Response
                object data.
            :type filename: string
            :param gb_var: global variable used referencing the current
                DAG pipeline name. This parameter exists because Airflow
                gives errors when using the `Variable` class to test locally
                for setting/getting, as it requires Airflow be already running.
                Python's os.environ property is used, in its place, during unit
                tests for set/getting environ variables. However, os.environ
                variables don't seem to carry over between Airflow tasks.
                (Using either Airflow's Variable or XCom classes might be more
                ideal here.)
            :type gb_var: string
        """

        log.info("Running get_news method")

        # check the status code, if is valid OK then save the result into
        # the appropriate news directory.
        status_code = response.status_code

        # retrieve the context-specific pipeline name from the upstream tasks
        if not gb_var:
            pipeline_name = Variable.get("current_dag_id")
        else:
            pipeline_name = gb_var

        # assign a default directory to store the data
        if not news_dir:
            news_dir = FileStorage.get_news_directory(pipeline_name)

        # assign a default filename for the data
        fname = filename
        if not filename:
            fname = "english_news_sources"

        # copy of the json data
        json_data = response.json()

        # write the data to file
        if status_code == requests.codes.ok:
            FileStorage.write_json_to_file(data=json_data,
                                           path_to_dir=news_dir,
                                           filename=fname)
            # airflow logging
            log.info("Files in Directory {} :-".format(news_dir))
            log.info(os.listdir(news_dir))

            return [True, status_code]
        elif status_code >= 400:
            return [False, status_code]
        else:
            return [False, status_code]

    @classmethod
    def get_news_headlines(cls, **context):
        """macro function for the Airflow PythonOperator that processes
        the retrieved upstream news json data into top-headlines.

        Operations Performed:

        extract the top-headlines and save them to a 'headlines' folder by:

        - getting the context-specific news directory (get_news_directory)

        - for each json file in that directory
           - read the file (json.load)
           - get the news sources id and put them in a list (extract source-id)

        - for each source id in the list
           - make httpcall to get its headlines as json (get_source_headlines)
           - extract the headlines (extract_news_headlines)
           - put them into a json (create_headline_json_func)
           - write the json to the 'headlines' directory (write_json_to_file)

        # Arguments:
            :param context: airflow context object of the currently running
                pipeline.
            :type context: dict
        """

        log.info("Running get_news_headlines method")

        # reference to the news api key
        apikey = config.NEWS_API_KEY

        # grab details about the current dag pipeline runnning
        dag_id = str(context['dag'].dag_id)
        pipeline_info = NewsInfoDTO(dag_id)

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        source_headlines_writer = FileStorage.write_source_headlines_to_file
        source_extract_func = ExtractOperations.extract_jsons_source_info

        # extract the news source tag information from jsons in the directory
        source_info = source_extract_func(pipeline_info.news_files,
                                          pipeline_info.news_directory)

        # ensure the last extraction step really worked before proceeding
        if not source_info:
            raise ValueError("No results from news source extraction")

        # reference to tuple of the list of each news source ids and names.
        extracted_ids = source_info[0]
        extracted_names = source_info[1]

        # get the headlines of sources, write them to json files. Note status.
        write_stat = source_headlines_writer(extracted_ids,
                                             extracted_names,
                                             pipeline_info.headlines_directory,
                                             apikey)

        # PythonOperator callable needs to return True or False status.
        return write_stat

    @classmethod
    def get_news_keyword_headlines(cls,
                                   response: requests.Response,
                                   headlines_dir=None,
                                   filename=None):
        """processes the response from the remote API call to get keyword headlines.

        Used by the SimpleHTTPOperator exclusively in the DAG pipeline
        'tempus_bonus_challenge_dag'.

        # Arguments:
            :param response: http response object returned from the
                SimpleHTTPOperator http call.
            :type response: Response object
            :param headlines_dir: directory to store the news data to.
            :type headlines_dir: string
            :param filename: name of the json file created from the Response
                object data.
            :type filename: string
        """

        log.info("Running get_news_keyword_headlines method")

        # extract the string query keyword used to request this headline
        query = ExtractOperations.extract_headline_keyword(response)

        # use the extracted query-keyword to construct the filename of the
        # final json file
        if not filename:
            filename = str(query) + "_headlines"

        # retrieve the path to the headlines directory of this
        # 'tempus_bonus_challenge' pipeline
        pipeline_name = "tempus_bonus_challenge_dag"
        pipeline_info = NewsInfoDTO(pipeline_name)

        if not headlines_dir:
            headlines_dir = pipeline_info.headline_directory

        # retrieve the json data from the Response object
        data = response.json()

        # write to json data to a file with the query-keyword
        # as its filename. Note status of the operation.
        # True implies the write went okay, False otherwise.
        write_stat = FileStorage.write_json_to_file(data,
                                                    headlines_dir,
                                                    filename)

        # file-write was successful and 'headlines' folder contains the json
        if write_stat and os.listdir(headlines_dir):
            return True
        else:
            return False

    @classmethod
    def get_source_headlines(cls,
                             source_id,
                             url_endpoint=None,
                             http_method=None,
                             api_key=None):
        """retrieve a news source's top-headlines via a remote API call.

        # Arguments:
            :param source_id: the id of the news source.
            :type source_id: string
            :param url_endpoint: the news api source url address. If not filled
                in the default News API sources endpoint is used.
            :type url_endpoint: string
            :param http_method: the Python function to use for making the
                remote call. If not filled in the default Python Request
                Library's get() method is used.
            :type http_method: function
            :param api_key: the News API Key for using the News API service.
            :type api_key: string

        # Raises:
            ValueError: if no news source id is passed in.
            ValueError: if no News API Key is passed in

        """

        log.info("Running get_source_headlines method")

        if not source_id:
            raise ValueError("'source_id' cannot be left blank")

        if not api_key:
            raise ValueError("No News API Key found")

        if not http_method:
            http_method = requests.get

        if not url_endpoint:
            url_endpoint = "https://newsapi.org/v2/top-headlines?"

        # craft the http request
        params = "sources=" + source_id
        key = "apiKey=" + api_key
        header = "".join([url_endpoint, params])
        full_request = "&".join([header, key])

        response = http_method(full_request)

        return response


class ExtractOperations:
    """handles functionality for extracting headlines."""

    @classmethod
    def create_top_headlines_json(cls, source_id, source_name, headlines):
        """creates a json object out of given news source and its headlines.

        # Arguments:
            :param source_id: the id of the news source
            :type source_id: string
            :param source_name: the name of the news source
            :type source_name: string
            :param headlines: list containing all the headlines of the news
                source, each as a string.
            :type headlines: list

        # Raises:
            ValueError: if the no source_id is given
            ValueError: if the no source_name is given
            ValueError: if the no headlines is given
        """

        log.info("Running create_top_headlines_json method")

        if not source_id:
            raise ValueError("'source_id' cannot be blank")
        if not source_name:
            raise ValueError("'source_name' cannot be blank")
        if not headlines:
            raise ValueError("'headlines' cannot be blank")

        source_top_headlines = {"source": {
                                "id": source_id,
                                "name": source_name},
                                "headlines": headlines}

        return source_top_headlines

    @classmethod
    def extract_news_source_id(cls, json_data):
        """returns a list of (string) news source ids from a valid json.

        # Arguments:
            :param json_data: the json news data from which the news-source
                ids will be extracted from.
            :type json_data: dict

        # Raises:
            KeyError: if the given json news data does not have the 'sources'
                tag.
            ValueError: if the given json news data has a 'sources' tag with
                empty data
        """

        log.info("Running extract_news_source_id method")

        if "sources" not in json_data.keys():
            raise KeyError("news json has no 'sources' data")

        if not json_data["sources"]:
            raise ValueError("'sources' tag in json is empty")

        sources_ids = []
        sources_names = []

        for source in json_data["sources"]:
            ids = source["id"]
            sources_ids.append(str(ids).lower())
            name = source["name"]
            sources_names.append(str(name).lower())

        return sources_ids, sources_names

    @classmethod
    def extract_news_headlines(cls, json_data):
        """returns a list of (string) news headlines from a valid json.

        # Arguments:
            :param json_data: the json news data from which the news-headlines
                will be extracted from.
            :type json_data: dict

        # Raises:
            KeyError: if the given json news data has no news 'articles'
            ValueError: if the given json news headlines data has a
                'articles' tag with empty data.
        """

        log.info("Running extract_news_headlines method")

        if "articles" not in json_data.keys():
            raise KeyError("news json has no 'articles' data")

        if not json_data["articles"]:
            print("There are no headlines - 'articles' tag in json is empty")
            return []

        # Get all the articles for this news source
        news_articles = [article for article in json_data["articles"]]

        # Get the headline of each news article
        headlines = [article["title"] for article in news_articles]

        return headlines

    @classmethod
    def extract_headline_keyword(cls, response: requests.Response):
        """extract string query keyword used to request given http Response.

        It specficially stores the query keyword, from the http Response
        object, that was applied during the http request to the 'top-headlines'
        News API endpoint.
        """

        log.info("Running extract_headline_keyword method")

        # inspect the request's Response object URL
        relative_url = response.request.path_url
        full_url = response.request.url

        # parse the url
        remove_relative_base_url = relative_url.split("?")
        # base_url = remove_relative_base_url[0]
        url_params = remove_relative_base_url[1]

        # extract the parameters
        parameter_list = []
        if '&' in url_params:
            parameter_list = url_params.split("&")
        else:
            parameter_list.append(url_params)

        # extract the query parameter 'q'
        query = [key for key in parameter_list if key.startswith('q')]

        # error check
        if not query:
            raise KeyError("Query param not found in URL {}".format(full_url))

        # there should only be one keyword variable in the url
        keyword = query[0].split("=")[1]
        query_keyword = str(keyword)

        return query_keyword.lower()

    @classmethod
    def extract_jsons_source_info(cls, json_list, json_directory):
        """parses a given list of news jsons for their source ids and names.

        Returns a tuple of the source id and name.

        # Arguments:
            :param json_list: list of jsons whose source info is to be parsed
            :type json_list: list
            :param json_directory: directory where these json files are stored.
            :type json_list: string

        # Raises:
            ValueError: if an error during parsing a json file is found
        """

        log.info("Running extract_jsons_source_info method")

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        source_extract_func = ExtractOperations.extract_news_source_id

        # process the collated json files
        for js in json_list:
            json_path = os.path.join(json_directory, js)

            # read each news json and extract the news sources
            with open(json_path, "r") as js_file:
                try:
                    raw_data = json.load(js_file)
                    extracted_sources = source_extract_func(raw_data)
                except ValueError:
                    raise ValueError("Parsing Error: {}".format(json_path))

        return extracted_sources

    @classmethod
    def extract_news_data_from_dataframe(cls, frame):
        """returns extracted information from a news dataframe.

        Based on the API documentation for the top-headlines, for the
        returned response, the fields that we need from each json object
        are:

        the identification id of the news source
        - source id

        the name of the news source
        - source name

        the author of the new article
        - author

        the headline or title of the article
        - title

        description from the article
        - description

        url to the article
        - url

        a url to a relevant image for the article
        - urlToImage

        the date and time the article was published
        - publishedAt

        the unformatted content of the article
        - content

        Returns a dictionary containing all these extracted information.

        # Arguments:
            :param frame: a Pandas DataFrame containing news data
            :type frame: DataFrame
        """

        log.info("Running extract_news_data_from_dataframe method")

        num_of_articles = frame['totalResults'][0]
        # dictionary representing the extracted news data
        extracted_data = {}

        # error check - no articles means this json had no news data
        if num_of_articles < 1:
            return extracted_data

        # Using Pandas, extract required information from the given dataframe
        # each is a return list of data.
        source_id = [frame['articles'][index]['source']['id']
                     for index in np.arange(num_of_articles)]

        source_name = [frame['articles'][index]['source']['name']
                       for index in np.arange(num_of_articles)]

        author = [frame['articles'][index]['author']
                  for index in np.arange(num_of_articles)]

        title = [frame['articles'][index]['title']
                 for index in np.arange(num_of_articles)]

        description = [frame['articles'][index]['description']
                       for index in np.arange(num_of_articles)]

        url = [frame['articles'][index]['url']
               for index in np.arange(num_of_articles)]

        url_to_image = [frame['articles'][index]['urlToImage']
                        for index in np.arange(num_of_articles)]

        published_at = [frame['articles'][index]['publishedAt']
                        for index in np.arange(num_of_articles)]

        content = [frame['articles'][index]['content']
                   for index in np.arange(num_of_articles)]

        # compose a dictionary with the extracted information
        extracted_data = {'source_id': source_id,
                          'source_name': source_name,
                          'author': author,
                          'title': title,
                          'description': description,
                          'url': url,
                          'url_to_image': url_to_image,
                          'published_at': published_at,
                          'content': content}

        return extracted_data


class NewsInfoDTO:
        """information about the news data this pipeline uses.


        This class functions as a Data Transfer Object(DTO).

        The get_news_headlines() function was refactored after discovering
        that alot of its functionalities were shared with that of the function
        get_news_keyword_headlines(). In order to create a common interface to
        write to at the end of the Extraction phase (the 'E' in ETL) the author
        decided to create a separate class which abstracted much of the shared
        functionality.
        Refactoring the get_news_headlines() function also made it easier to
        unit test as well.

        # Arguments:
            :param json_data: the json news data from which the news-headlines
                will be extracted from.
            :type json_data: dict
            :param pipeline_name: name of the current DAG pipeline.
            :type pipeline_name: string
            :param dir_check_func: function that returns news directory of
                the pipeline
            :type dir_check_func: function

        # Raises:
            ValueError: if the required 'json_data' argument is left blank
            ValueError: if the required 'pipeline_name' argument is left blank
        """

        def __init__(self, pipeline_name, dir_check_func=None):
            valid_dags = ['tempus_challenge_dag', 'tempus_bonus_challenge_dag']

            if not pipeline_name:
                raise ValueError("Argument pipeline_name cannot be left blank")

            if pipeline_name not in valid_dags:
                raise ValueError("{} not valid pipeline".format(pipeline_name))

            self.pipeline = str(pipeline_name)

            # for the 'tempus_challenge_dag' pipeline we need to retrieve the
            # collated news sources json files from the upstream task
            self.news_json_files = []
            if self.pipeline == "tempus_challenge_dag":
                self.news_json_files = self.load_news_files(dir_check_func)

        @property
        def headlines_directory(self) -> str:
            """returns the path to this pipeline's headline directory."""
            return FileStorage.get_headlines_directory(self.pipeline)

        @property
        def news_directory(self) -> str:
            """returns the path to this pipeline's news directory."""
            return FileStorage.get_news_directory(self.pipeline)

        @property
        def csv_directory(self) -> str:
            """returns the path to this pipeline's csv directory."""
            return FileStorage.get_csv_directory(self.pipeline)

        @property
        def news_files(self) -> list:
            """returns json files in the news directory of this pipeline."""
            return self.news_json_files

        def load_news_files(self, dir_check_func=None):
            """get the contents of the pipeline's news directory."""

            files = []
            if not dir_check_func:
                dir_check_func = self.news_directory()

            if dir_check_func and os.listdir(dir_check_func):
                for data_file in os.listdir(dir_check_func):
                    if data_file.endswith('.json'):
                        files.append(data_file)
            return files


class TransformOperations:
    """handles functionality for flattening CSVs."""

    @classmethod
    def transform_headlines_to_csv(cls, info_func=None, **context):
        """converts the jsons in a given directory to csv.

        Use different transformation methods depending on the
        current active pipeline.

        For the 'tempus_challenge_dag' pipeline, the function
        `transform_news_headlines_to_csv` is used via a helper-function
        `helper_execute_json_transformation`.

        For the 'tempus_bonus_challenge_dag' pipeline, the function
        `transform_keyword_headlines_to_csv` is used via a helper-function
        `helper_execute_keyword_json_transformation`.

        The end transformations are stored in the respective 'csv'
        datastore folders of the respective pipelines.

                Pipeline 1  CSV: Tempus Challenge DAG
        For the 'tempus_challenge_dag' pipeline all the news headlines
        from all the english sources are flattened and transformed into
        one single csv file, the pipeline execution date is appended to
        the end transformed csv. It is of the form:
        `pipeline_execution_date`_headlines.csv

                Pipeline 2 CSVs: Tempus Bonus Challenge DAG
        For each the four keywords queries of the 'tempus_bonus_challenge_dag'
         - 'Tempus Labs', 'Eric Lefkofsky', 'Cancer', 'Immunotheraphy' - the
        result is four separate csv files, each representing all the headlines
        about that particular keyword. The pipeline execution date is appended
        to the end transformed csv's. The keyword headline files are of form:
        `pipeline_execution_date`_`keyword`_`headlines`.csv

        #  Arguments:
            :param info_func: function used to provide more information
                about the current pipeline
            :type info_func: function
            :param context: Airflow context object reference to the current
                pipeline.
            :type info_func: dict
        """

        log.info("Running transform_headlines_to_csv method")

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        transform_json_fnc = cls.helper_execute_json_transformation
        transform_key_json_fnc = cls.helper_execute_keyword_json_transformation

        # get active pipeline information
        pipeline_name = context['dag'].dag_id
        if not info_func:
            info_func = NewsInfoDTO
        pipeline_info = info_func(pipeline_name)
        headline_dir = pipeline_info.get_headlines_directory

        # execution date of the current pipeline
        exec_date = str(context['execution_date'])

        # transformation operation status
        transform_status = None

        # perform context-specific transformations
        if pipeline_name == "tempus_challenge_dag":
            # transform all jsons in the 'headlines' directory
            transform_status = transform_json_fnc(headline_dir, exec_date)
            return transform_status
        elif pipeline_name == "tempus_bonus_challenge_dag":
            # transform all jsons in the 'headlines' directory
            transform_status = transform_key_json_fnc(headline_dir, exec_date)
            return transform_status
        else:
            # the active pipeline is not one of the two we developed for.
            print("This pipeline {} is not valid".format(pipeline_name))
            # log this issue in Airflow and return an error status
            log.info("This pipeline {} is not valid".format(pipeline_name))
            return False

    @classmethod
    def helper_execute_keyword_json_transformation(cls,
                                                   directory,
                                                   timestamp=None):
        """runs a block of code to transform keyword json headlines to csv.

        # Arguments:
            :param directory: directory having the jsons to
                execute a transformation on.
            :type directory: string
            :param timestamp: date of the pipeline execution that
                should be appended to created csv files
            :type timestamp: datetime object
        """

        log.info("Running helper_execute_keyword_json_transformation method")

        # transformation operation status
        status = None

        # the name the created csv file should be given
        fname = None

        # execution date cannot be None
        if not timestamp:
            timestamp = datetime.datetime.now()

        # transform individual jsons in the 'headlines' directory into
        # individual csv files
        files = None

        if os.listdir(directory):
            files = [file for file in os.listdir(directory)
                     if file.endswith('.json')]

        # check existence of json files before beginning transformation
        if not files:
            raise NoFilesFoundError("Directory has no json-headline files")
        else:
            for file in files:
                key = file.split("_")[1]
                fname = str(timestamp) + "_" + key + "_top_headlines.csv"
                status = cls.transform_keyword_headlines_to_csv(file, fname)

        return status

    @classmethod
    def helper_execute_json_transformation(cls,
                                           directory,
                                           timestamp=None,
                                           json_to_csv_func=None,
                                           jsons_to_df_func=None,
                                           df_to_csv_func=None):
        """runs a block of code to transform json headlines to csv.


        A number of performance issues need to be considered for this task:

        The intent of the code is combined multiple jsons files into one csv.
        This could be done using Pandas's DataFrame object
        as an intermediary format - performing the merge in DataFrames
        as a stream OR as a batch (both which have their memory tradeoffs)
        This could also be done using csv's - by converting each json file
        # into a csv and doing the merger there (also as either stream OR
        # batch) using for example Python's CSV.

        Memory Tradeoffs Notes:
        We work with the assumption that processing all these jsons together
        at once isn't possible - especially in case of json data sets that
        don't fit into memory (even if we optimized types and filtered some
        of the data). In this instance, a better strategy will be to make the
        transformation in chunks (or batches); turning a portion into
        DataFrames in memory and merging, till we have all the jsons merged
        as a single DataFrame.


        Though the author is familiar with both Pandas and CSV libraries,
        I decided to use Pandas - due to it being, in my opinion, more
        powerful than the mere CSV library. Pandas also uses the CSV library
        internal and has highly flexible functions for manipulating both
        json and csv data.

        I decided to use Pandas's DataFrame object as the intermediary format
        and a Batch merging approach: Transforming the json's, two at a time
        (i.e. pairwise merging), into DataFrames and merging them, after which
        the old DataFrames are cleaned up in memory before the next
        transform-merge iteration; till the whole transformed jsons are merged
        into one single DataFrame.

        The time complexity of doing a sequential merge (of the news json
        files) is O(n) which would become a problem to do as the number of
        files grow.

        An alternative approach follows from the principles in the traditional
        Merge Sort algorithm - whose time complexity in the best, average, and
        worst cases are O(n logn) - Space complexity worst case is O(n)
        http://bigocheatsheet.com/

        From an algorithmic time complexity standpoint, however, O(n) is better
        than O(n logn). But, more efficient merge-method will be needed when
        dealing with larger file sizes.

        # Arguments:
            :param directory: directory having the jsons to
                execute a transformation on.
            :type directory: string
            :param timestamp: date of the pipeline execution that
                should be appended to created csv files
            :type timestamp: datetime object
            :param json_to_csv_func: function that transforms a single news
                json file into a csv
            :type json_to_csv_func: function
            :param jsons_to_df_func: function that transforms a set of news
                json files into intermediary DataFrames and merges them into
                one final DataFrame
            :type jsons_to_df_func: function
            :param df_to_csv_func: function that transforms a single DataFrame
                into a csv file
        """

        log.info("Running helper_execute_json_transformation method")

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        if not json_to_csv_func:
            json_to_csv_func = cls.transform_new_headlines_json_to_csv
        if not jsons_to_df_func:
            jsons_to_df_func = cls.transform_jsons_to_dataframe_merger
        if not df_to_csv_func:
            df_to_csv_func = cls.transform_news_headlines_to_csv

        # transformation operation status
        status = None

        # execution date cannot be None
        if not timestamp:
            timestamp = datetime.datetime.now()

        # the name the created csv file should be given
        filename = str(timestamp) + "_top_headlines.csv"

        # To perform continous pairwise merging of the dataframe-transformed
        # json files in the directory, we need a way to keep track of what has
        # been merged so far. There needs to be a way to maintain state.
        # This (empty) DataFrame is created for that purpose and is made into
        # a global Python object within this module/python file.
        #
        # Use of global variables might not be the ideal way to handle this, as
        # they are generally discouraged in several development
        # environs/instances due to the kinds of unpredictable bugs they
        # potentially create - most especially in multithreaded environments)
        #
        # Hence, our use of the `global` keyword here for this operation is
        # ONLY time it will ever be used in this project.
        #
        # An alternative, to consider, might be to make use of Airflow's
        # Variable class to store the state of object.
        #
        # Another alternative, to consider, asides global variables is the use
        # of Python (function) closures to achieve state maintenance.
        merged_df = pd.DataFrame()

        # transform individual jsons in the 'headlines' directory into one
        # single csv file
        files = []

        if not os.listdir(directory):
            raise FileNotFoundError("Directory is empty")

        if os.listdir(directory):
            files = [file for file in os.listdir(directory)
                     if file.endswith('.json')]

        # check existence of json files before beginning transformation
        if not files:
            raise FileNotFoundError("Directory has no json-headline files")

        if len(files) == 1:
            # a single json file exists, perform direct transformation on it.
            status = json_to_csv_func(files[0], filename)
        else:
            # transform the json files into DataFrames and merge them into one.
            merged_df = jsons_to_df_func(files)
            # transform the merged DataFrame into a csv
            status = df_to_csv_func(merged_df, filename)

        return status

    @classmethod
    def transform_jsons_to_dataframe_merger(cls, json_files):
        """transforms a set of json files into a DataFrames and merges all of
        them into one.

        # Arguments:
            :param json_files: a list of json files to be processed.
            :type json_files: list
        """

        log.info("Running transform_jsons_to_dataframe_merger method")

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        extr_frm_frame_fnc = ExtractOperations.extract_news_data_from_dataframe
        trans_to_frame_fnc = TransformOperations.transform_data_to_dataframe
        read_js_fnc = pd.read_json

        # perform pairwise transformation of the json files into DataFrames
        # and their subsequent merging into a single DataFrame.
        current_file_df = None
        next_file_df = None

        # setup final DataFrame that will be resulting from the entire merge
        global merged_df

        for indx, file in enumerate(json_files):
            if indx == (len(json_files) - 1):
                break
            # perform json to DataFrame transformations by function-chaining
            current_file_df = trans_to_frame_fnc(extr_frm_frame_fnc(
                                                 read_js_fnc(json_files[indx])
                                                 ))
            next_file_df = trans_to_frame_fnc(extr_frm_frame_fnc(
                                              read_js_fnc(json_files[indx + 1])
                                              ))
            # perform merge
            merged_df = pd.concat([merged_df, current_file_df, next_file_df])
            # free up memory by clearing the previously transformed DataFrames
            del current_file_df
            del next_file_df
            # force Python's Garbage Collector to clean up unused variables and
            # free up memory manually
            gc.collect()

        # return a merged DataFrame of all the jsons
        return merged_df

    @classmethod
    def transform_new_headlines_json_to_csv(cls, json_file, csv_filename=None):
        """transforms the json contents of a given file into a csv.

        The function specifically operates on jsons in the 'headlines'
        folder of the 'tempus_challenge_dag' pipeline.

        Uses the Pandas library to parse, traverse and flatten the
        json data into a csv file.

        # Arguments:
            :param json_file: a json file containing top news headlines
                based on a keyword
            :type json_file: file
            :param csv_filename: the filename of the transformed csv
            :type csv_filename: string
        """

        log.info("Running transform_new_headlines_single_file_to_csv method")

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        extr_frm_frame_fnc = ExtractOperations.extract_news_data_from_dataframe
        trans_to_frame_fnc = TransformOperations.transform_data_to_dataframe

        # use Pandas to read in the json file
        keyword_data = pd.read_json(json_file)

        # extraction and intermediate-transformation of the news json
        data = extr_frm_frame_fnc(keyword_data)
        transformed_df = trans_to_frame_fnc(data)

        # transform to csv and save in the 'csv' datastore
        csv_dir = FileStorage.get_csv_directory("tempus_challenge_dag")
        if not csv_filename:
            csv_filename = "sample.csv"
        csv_save_path = os.path.join(csv_dir, csv_filename)
        transformed_df.to_csv(path_or_buf=csv_save_path)

        # ensure status of operation is communicated to caller function
        op_status = None
        if os.listdir(csv_dir):
            log.info("english news headlines csv saved in {}".format(csv_dir))
            op_status = True
        else:
            op_status = False

        return op_status

    @classmethod
    def transform_news_headlines_to_csv(cls, frame, csv_filename):
        """converts the json contents of a given folder into a csv.

        The function specifically operates on jsons in the 'headlines'
        fold of the 'tempus_bonus_challenge_dag' pipeline.

        Uses the Pandas library to parse, traverse and flatten the
        json data into a csv file.

         # Arguments:
            :param frame: single DataFrame consisting of all english news
                sources headlines
            :type frame: DataFrame
            :param csv_filename: the filename of the transformed csv
            :type csv_filename: string
        """

        log.info("Running transform_news_headlines_to_csv method")

        # input is a single DataFrame consisting of all english news sources
        # headlines
        transformed_df = frame

        # transform to csv and save in the 'csv' datastore
        csv_dir = FileStorage.get_csv_directory("tempus_challenge_dag")
        if not csv_filename:
            csv_filename = "sample.csv"
        csv_save_path = os.path.join(csv_dir, csv_filename)
        transformed_df.to_csv(path_or_buf=csv_save_path)

        # ensure status of operation is communicated to caller function
        op_status = None
        if os.listdir(csv_dir):
            log.info("english news headlines csv saved in {}".format(csv_dir))
            op_status = True
        else:
            op_status = False

        return op_status

    @classmethod
    def transform_keyword_headlines_to_csv(cls, json_file, csv_filename=None):
        """converts the json contents of a given folder into a csv.

        The function specifically operates on jsons in the 'headlines'
        folder of the 'tempus_bonus_challenge_dag' pipeline.

        Uses the Pandas library to parse, traverse and flatten the
        json data into a csv file.

        # Arguments:
            :param json_file: a json file containing top news headlines
                based on a keyword
            :type json_file: file
            :param csv_filename: the filename of the transformed csv
            :type csv_filename: string
        """

        log.info("Running transform_keyword_headlines_to_csv method")

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        extr_frm_frame_fnc = ExtractOperations.extract_news_data_from_dataframe
        trans_to_frame_fnc = TransformOperations.transform_data_to_dataframe

        # use Pandas to read in the json file
        keyword_data = pd.read_json(json_file)

        # extraction and intermediate-transformation of the news json
        data = extr_frm_frame_fnc(keyword_data)
        transformed_df = trans_to_frame_fnc(data)

        # transform to csv and save in the 'csv' datastore
        csv_dir = FileStorage.get_csv_directory("tempus_bonus_challenge_dag")
        if not csv_filename:
            csv_filename = "sample.csv"
        csv_save_path = os.path.join(csv_dir, csv_filename)
        transformed_df.to_csv(path_or_buf=csv_save_path)

        # ensure status of operation is communicated to caller function
        op_status = None
        query_key = csv_filename.split("_")[1]
        if os.listdir(csv_dir):
            log.info("{} headlines csv saved in {}".format(query_key, csv_dir))
            op_status = True
        else:
            op_status = False

        return op_status

    @classmethod
    def transform_data_to_dataframe(cls, news_data):
        """converts a dictionary of news data into a Pandas Dataframe.

        # Arguments:
            :param news_data: extracted news data information.
            :type news_data: dict
        """

        log.info("Running transform_data_to_dataframe method")

        # error-check
        if not news_data:
            raise ValueError("news data argument cannot be empty")

        field_names = ['news_source_id',
                       'news_source_name',
                       'news_author',
                       'news_title',
                       'news_description',
                       'news_url',
                       'news_image_url',
                       'news_publication_date',
                       'news_content']

        # craft the transformed dataframe
        news_df = pd.DataFrame()

        # populate the columns of the dataframe with news data
        for index, field in enumerate(list(news_data.keys())):
            news_df[field_names[index]] = news_data[field]

        return news_df


class UploadOperations:
    """handles functionality for uploading flattened CSVs.

    Reads a 'csv' directory's files and uploads them to
    an Amazon S3 bucket using the S3 Hook and boto library.
    """

    @classmethod
    def upload_csv_to_s3(cls, **context):
        """uploads all the files in a given directory to an Amazon S3
        bucket location."""

        pass


def process_retrieved_data(self):
    """for each news performs a series of ETL operations."""
