
import logging
import os
import requests

from airflow.models import Variable

from dags import challenge as c


log = logging.getLogger(__name__)


class NetworkOperations:
    """Handles functionality for making remote calls to the News API."""

    # store the current directory of the airflow home folder
    # airflow creates a home environment variable pointing to the location
    HOME_DIRECTORY = str(os.environ['HOME'])

    @classmethod
    def get_news(cls,
                 response: requests.Response,
                 news_dir=None,
                 filename=None,
                 gb_var=None):
        """Processes the response from a remote API call to get english news sources.

        Returns True if the response is valid and stores the content in the
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
            :type news_dir: str
            :param filename: name of the json file created from the Response
                object data.
            :type filename: str
            :param gb_var: global variable used referencing the current
                DAG pipeline name. This parameter exists because Airflow
                gives errors when using the `Variable` class to test locally
                for setting/getting, as it requires Airflow be already running.
                Python's os.environ property is used, in its place, during unit
                tests for set/getting environ variables. However, os.environ
                variables don't seem to carry over between Airflow tasks.
                (Using either Airflow's Variable or XCom classes might be more
                ideal here.)
            :type gb_var: str
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
            news_dir = c.FileStorage.get_news_directory(pipeline_name)

        # assign a default filename for the data
        fname = filename
        if not filename:
            fname = "english_news_sources"

        # copy of the json data
        json_data = response.json()

        # write the data to file
        if status_code == requests.codes.ok:
            c.FileStorage.write_json_to_file(data=json_data,
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
        apikey = os.environ['NEWS_API_KEY']

        # grab details about the current dag pipeline runnning
        dag_id = str(context['dag'].dag_id)
        pipeline_info = c.NewsInfoDTO(dag_id)

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        source_headlines_writer = c.FileStorage.write_source_headlines_to_file
        source_extract_func = c.ExtractOperations.extract_jsons_source_info

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
            :type headlines_dir: str
            :param filename: name of the json file created from the Response
                object data.
            :type filename: str
        """

        log.info("Running get_news_keyword_headlines method")

        # extract the string query keyword used to request this headline
        query = c.ExtractOperations.extract_headline_keyword(response)

        # use the extracted query-keyword to construct the filename of the
        # final json file
        if not filename:
            filename = str(query) + "_headlines"

        # retrieve the path to the headlines directory of this
        # 'tempus_bonus_challenge' pipeline
        pipeline_name = "tempus_bonus_challenge_dag"
        pipeline_info = c.NewsInfoDTO(pipeline_name)

        if not headlines_dir:
            headlines_dir = pipeline_info.headline_directory

        # retrieve the json data from the Response object
        data = response.json()

        # write to json data to a file with the query-keyword
        # as its filename. Note status of the operation.
        # True implies the write went okay, False otherwise.
        write_stat = c.FileStorage.write_json_to_file(data,
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
            :type source_id: str
            :param url_endpoint: the news api source url address. If not filled
                in the default News API sources endpoint is used.
            :type url_endpoint: str
            :param http_method: the Python function to use for making the
                remote call. If not filled in the default Python Request
                Library's get() method is used.
            :type http_method: function
            :param api_key: the News API Key for using the News API service.
            :type api_key: str

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
