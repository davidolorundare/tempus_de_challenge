"""Tempus challenge  - Unit Tests.

Defines unit tests for underlining functions to operators of tasks in the DAGs.


"""


import datetime
import json
import os


from unittest.mock import MagicMock
from unittest.mock import patch

from airflow.models import DAG
# from airflow.models import Variable

from dags import challenge as c
from dags import env

from pyfakefs.fake_filesystem_unittest import Patcher

import pytest

import requests


class MissingApiKeyError(ValueError):
    """raised when no api key is found or set"""
    pass


@pytest.mark.storagetests
class TestFileStorage:
    """Tests the creation of the tempoary datastores used during ETL tasks.

    Maybe mock and test that os.path.exists(directory_path) is False before
    the call and True afterwards.
    test if directory already exists after the call. VERY IMPORTANT!
    """

    @pytest.fixture(scope='class')
    def home_directory_res(self) -> str:
        """returns a pytest resource - path to the Airflow Home directory."""
        return str(os.environ['HOME'])

    @pytest.fixture(scope='class')
    def data_directories_res(self) -> list:
        """returns a pytest resouce: list of data directories names."""
        return ['news', 'headlines', 'csv']

    @pytest.fixture(scope='class')
    def airflow_context(self) -> dict:
        """returns an airflow context object for tempus_challenge_dag.

        Mimics parts of the airflow context returned during execution
        of the tempus_challenge_dag.

        https://airflow.apache.org/code.html#default-variables
        """

        dag = MagicMock(spec=DAG)
        dag.dag_id = "tempus_challenge_dag"

        return {
            'ds': datetime.datetime.now().isoformat().split('T')[0],
            'dag': dag
        }

    @pytest.fixture(scope='class')
    def airflow_context_bonus(self) -> dict:
        """return an airflow context object for tempus_bonus_challange_dag.

        Mimics parts of the airflow context returned during execution
        of the tempus_bonus_challenge_dag.

        https://airflow.apache.org/code.html#default-variables
        """

        dag = MagicMock(spec=DAG)
        dag.dag_id = "tempus_bonus_challenge_dag"

        return {
            'ds': datetime.datetime.now().isoformat().split('T')[0],
            'dag': dag
        }

    @patch('os.makedirs', autospec=True)
    @patch('os.path.join', autospec=True)
    def test_create_news_data_store_pipe1_success(self,
                                                  mock_path_func,
                                                  mock_dir_func,
                                                  home_directory_res,
                                                  data_directories_res,
                                                  airflow_context):
        """call to create the tempoary 'news' datastore folders used by the
        tempus_challenge_dag operators succeeds.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_stores("news",
                                         mock_path_func,
                                         mock_dir_func,
                                         **airflow_context)

        # Assert
        mock_path_func.assert_called_with(home_directory_res,
                                          'tempdata',
                                          'tempus_challenge_dag',
                                          data_directories_res[0])
        mock_path_func.reset_mock()
        mock_dir_func.assert_called_with(mock_path_func(
                                         home_directory_res,
                                         'tempdata',
                                         'tempus_challenge_dag',
                                         data_directories_res[0]),
                                         exist_ok=True)

    @patch('os.makedirs', autospec=True)
    @patch('os.path.join', autospec=True)
    def test_create_headlines_data_store_pipe1_success(self,
                                                       mock_path_func,
                                                       mock_dir_func,
                                                       home_directory_res,
                                                       data_directories_res,
                                                       airflow_context):
        """call to create the tempoary 'headlines' datastore folders used by the
        tempus_challenge_dag operators succeed.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_stores("headlines",
                                         mock_path_func,
                                         mock_dir_func,
                                         **airflow_context)

        # Assert
        mock_path_func.assert_called_with(home_directory_res,
                                          'tempdata',
                                          'tempus_challenge_dag',
                                          data_directories_res[1])
        mock_path_func.reset_mock()
        mock_dir_func.assert_called_with(mock_path_func(
                                         home_directory_res,
                                         'tempdata',
                                         'tempus_challenge_dag',
                                         data_directories_res[1]),
                                         exist_ok=True)

    @patch('os.makedirs', autospec=True)
    @patch('os.path.join', autospec=True)
    def test_create_csv_data_store_pipe1_success(self,
                                                 mock_path_func,
                                                 mock_dir_func,
                                                 home_directory_res,
                                                 data_directories_res,
                                                 airflow_context):
        """call to create the tempoary 'csv' datastore folders used by the
        tempus_challenge_dag operators.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_stores("csv",
                                         mock_path_func,
                                         mock_dir_func,
                                         **airflow_context)

        # Assert
        mock_path_func.assert_called_with(home_directory_res,
                                          'tempdata',
                                          'tempus_challenge_dag',
                                          data_directories_res[2])
        mock_path_func.reset_mock()
        mock_dir_func.assert_called_with(mock_path_func(
                                         home_directory_res,
                                         'tempdata',
                                         'tempus_challenge_dag',
                                         data_directories_res[2]),
                                         exist_ok=True)

    @patch('os.makedirs', autospec=True)
    @patch('os.path.join', autospec=True)
    def test_create_news_data_store_pipe2_success(self,
                                                  mock_path_func,
                                                  mock_dir_func,
                                                  home_directory_res,
                                                  data_directories_res,
                                                  airflow_context_bonus):
        """call to create the tempoary 'news' datastore folders used by the
        tempus_bonus_challenge_dag operators succeeds.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_stores("news",
                                         mock_path_func,
                                         mock_dir_func,
                                         **airflow_context_bonus)
        # Assert
        mock_path_func.assert_called_with(home_directory_res,
                                          'tempdata',
                                          'tempus_bonus_challenge_dag',
                                          data_directories_res[0])
        mock_path_func.reset_mock()
        mock_dir_func.assert_called_with(mock_path_func(
                                         home_directory_res,
                                         'tempdata',
                                         'tempus_bonus_challenge_dag',
                                         data_directories_res[0]),
                                         exist_ok=True)

    @patch('os.makedirs', autospec=True)
    @patch('os.path.join', autospec=True)
    def test_create_headlines_data_store_pipe2_success(self,
                                                       mock_path_func,
                                                       mock_dir_func,
                                                       home_directory_res,
                                                       data_directories_res,
                                                       airflow_context_bonus):
        """call to create the tempoary 'headlines' datastore folders used by the
        tempus_bonus_challenge_dag operators succeeds.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_stores("headlines",
                                         mock_path_func,
                                         mock_dir_func,
                                         **airflow_context_bonus)
        # Assert
        mock_path_func.assert_called_with(home_directory_res,
                                          'tempdata',
                                          'tempus_bonus_challenge_dag',
                                          data_directories_res[1])
        mock_path_func.reset_mock()
        mock_dir_func.assert_called_with(mock_path_func(
                                         home_directory_res,
                                         'tempdata',
                                         'tempus_bonus_challenge_dag',
                                         data_directories_res[1]),
                                         exist_ok=True)

    @patch('os.makedirs', autospec=True)
    @patch('os.path.join', autospec=True)
    def test_create_csv_data_store_pipe2_success(self,
                                                 mock_path_func,
                                                 mock_dir_func,
                                                 home_directory_res,
                                                 data_directories_res,
                                                 airflow_context_bonus):
        """call to create the tempoary 'csv' datastore folders used by the
        tempus_bonus_challenge_dag operators succeeds.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_stores("csv",
                                         mock_path_func,
                                         mock_dir_func,
                                         **airflow_context_bonus)
        # Assert
        mock_path_func.assert_called_with(home_directory_res,
                                          'tempdata',
                                          'tempus_bonus_challenge_dag',
                                          data_directories_res[2])
        mock_path_func.reset_mock()
        mock_dir_func.assert_called_with(mock_path_func(
                                         home_directory_res,
                                         'tempdata',
                                         'tempus_bonus_challenge_dag',
                                         data_directories_res[2]),
                                         exist_ok=True)

    @pytest.mark.skip
    # @patch('requests.Response', autospec=True)
    def test_file_sensors_detects_file_correctly(self, response_obj):
        """successful detection of a new file in a given directory."""

        # Arrange
        # Need to figure out the context
        # Act

        # Assert

    def test_get_news_dir_returns_correct_path(self, home_directory_res):
        """returns correct news path when called correctly with pipeline name.
        """

        # Arrange
        news_path = os.path.join(home_directory_res,
                                 'tempdata',
                                 'tempus_challenge_dag',
                                 'news')

        # Act
        path = c.FileStorage.get_news_directory("tempus_challenge_dag")

        # Assert
        assert path == news_path

    def test_get_headlines_dir_returns_correct_path(self, home_directory_res):
        """returns correct headlines path when called correctly with DAG name.
        """

        # Arrange
        news_path = os.path.join(home_directory_res,
                                 'tempdata',
                                 'tempus_bonus_challenge_dag',
                                 'headlines')

        pipeline = "tempus_bonus_challenge_dag"

        # Act
        path = c.FileStorage.get_headlines_directory(pipeline)

        # Assert
        assert path == news_path

    def test_get_csv_dir_returns_correct_path(self, home_directory_res):
        """returns correct csv path when called correctly with pipeline name.
        """

        # Arrange
        news_path = os.path.join(home_directory_res,
                                 'tempdata',
                                 'tempus_challenge_dag',
                                 'csv')

        # Act
        path = c.FileStorage.get_csv_directory("tempus_challenge_dag")

        # Assert
        assert path == news_path

    def test_write_json_to_file_succeeds(self):
        """successful write of json string data to a file directory."""

        # Arrange
        # dummy json data - Python object converted to json string
        json_data = json.dumps({'key': 'value'})
        # path to a directory that doesn't yet exist
        datastore_folder_path = "/data/"
        # detect presence of a file in the directory before and after
        # calling the method under test.
        file_is_absent = False
        file_is_present = False

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory to test the method
            patcher.fs.create_dir(datastore_folder_path)

            # ensure that the newly created directory is really empty
            if not os.listdir(datastore_folder_path):
                file_is_absent = True

            # Act
            # write some dummy json data into a file a save to that directory
            result = c.FileStorage.write_json_to_file(json_data,
                                                      datastore_folder_path,
                                                      filename="test")
            # inspect the directory - the json file should now be in there
            if os.listdir(datastore_folder_path):
                file_is_present = True

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert file_is_absent is True
        assert result is True
        assert file_is_present is True

    def test_write_json_to_file_fails_with_wrong_directory_path(self):
        """write of json data to a file to a non-existent directory
        fails correctly.
        """

        # Arrange
        # valid dummy json data
        json_data = json.dumps({'some_key': 'some_value'})

        # path to directory that doesn't yet exist
        datastore_folder_path = "/data/"

        # Act
        # Assert
        # writing some dummy json data into a file to save to that non-existent
        # directory should throw errors
        with pytest.raises(OSError) as err:
            c.FileStorage.write_json_to_file(json_data,
                                             datastore_folder_path,
                                             filename="test")
        expected = "Directory {} does not exist".format(datastore_folder_path)
        assert expected in str(err.value)

    def test_write_json_to_file_fails_with_bad_data(self):
        """write of a invalid json data to a file (already existent directory
        fails correctly.
        """

        # Arrange
        # invalid or bad json data
        # json_data = {u"my_key": u'\uda00'}  # int('-inf')

        # path to directory that doesn't yet exist but will be created
        datastore_folder_path = "/data/"

        # detect presence of a file in the directory before and after
        # calling the method under test.
        file_is_absent = False
        file_is_present = False

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory to test the method
            patcher.fs.create_dir(datastore_folder_path)

            # ensure that the newly created directory is really empty
            if not os.listdir(datastore_folder_path):
                file_is_absent = True

        # Act
            # writing some dummy bad json data into a file to save to that
            # existent directory should throw bad-input errors
            with pytest.raises(ValueError) as err:
                c.FileStorage.write_json_to_file(datastore_folder_path,
                                                 filename="test",
                                                 data=int('w'))

            actual_error = str(err.value)

            # inspect the directory - the method failed, the json file should
            # not be in there and file_is_present should remain False
            if os.listdir(datastore_folder_path):
                file_is_present = True

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert file_is_absent is True
        assert file_is_present is False
        assert "invalid literal" in actual_error

    @pytest.mark.skip(reason="wrong_directory_path test checks some of this")
    def test_write_json_to_file_fails_reading_with_io_error(self):
        """write of a json data to a file non-existent directory fails correctly.

        Requires commenting out the first check for directory in the code of
        the method - as that throws OSError (of which IOError is a child class)
        This test passes on its own in that scenario and ascertains that the
        the IOError-checking code that runs later in the method actually works.
        For that reason it is intentionally deactivated (marked as 'skip').

        The method *could* be however refactored to have just both checks,
        closer to each other. i.e. IOError and OSError.
        """

        # Arrange
        # invalid or bad json data that should cause I/O errors to read
        json_data = json.dumps({'another_key': 'another_value'})

        # path to directory that doesn't exist
        datastore_folder_path = "/data/"

        # Act
        # writing some dummy bad json data into a file to save to that
        # existent directory should throw I/O errors
        with pytest.raises(IOError) as err:
            c.FileStorage.write_json_to_file(json_data,
                                             datastore_folder_path,
                                             filename="test")

        actual_error = str(err.value)

        # Assert
        assert "Error in Reading Data - IOError" in actual_error

    @pytest.mark.skip(reason="not decided best way to get at the OSError yet")
    @patch('os.makedirs', autospec=True)
    @patch('os.path.join', autospec=True)
    def test_create_data_store_pipe1_failure(self,
                                             mock_path_func,
                                             mock_dir_func,
                                             home_directory_res,
                                             data_directories_res,
                                             airflow_context_bonus):
        """call to create the tempoary datastore folders used by the
        tempus_bonus_challenge_dag operators fails.
        """

        # Arrange
        # NEED TO REFACTOR THIS
        # Act
        c.FileStorage.create_data_stores("csv",
                                         mock_path_func,
                                         mock_dir_func,
                                         **airflow_context_bonus)
        # Assert
        mock_path_func.assert_called_with(home_directory_res,
                                          'tempdata',
                                          'tempus_bonus_challenge_dag',
                                          data_directories_res[2])
        mock_path_func.reset_mock()
        mock_dir_func.assert_called_with(mock_path_func(
                                         home_directory_res,
                                         'tempdata',
                                         'tempus_bonus_challenge_dag',
                                         data_directories_res[2]),
                                         exist_ok=True)

    @pytest.mark.skip(reason="not decided best way to get at the OSError yet.")
    @patch('os.makedirs', autospec=True)
    @patch('os.path.join', autospec=True)
    def test_create_data_store_pipe2_failure(self, mock_path_func,
                                             mock_dir_func,
                                             home_directory_res,
                                             data_directories_res,
                                             airflow_context_bonus):
        """call to create the tempoary datastore folders used by the
        tempus_bonus_challenge_dag operators fails."""

        # Arrange
        # NEED TO REFACTOR THIS
        # Act
        c.FileStorage.create_data_stores("csv",
                                         mock_path_func,
                                         mock_dir_func,
                                         **airflow_context_bonus)
        # Assert
        mock_path_func.assert_called_with(home_directory_res,
                                          'tempdata',
                                          'tempus_bonus_challenge_dag',
                                          data_directories_res[2])
        mock_path_func.reset_mock()
        mock_dir_func.assert_called_with(mock_path_func(
                                         home_directory_res,
                                         'tempdata',
                                         'tempus_bonus_challenge_dag',
                                         data_directories_res[2]),
                                         exist_ok=True)

    def test_get_news_directory_fails_with_wrong_name(self):
        """returns error when function is called with wrong pipeline name."""

        # Arrange
        # Act
        # Assert
        with pytest.raises(ValueError) as err:
            c.FileStorage.get_news_directory("wrong_name_dag")
        assert "No directory path for given pipeline name" in str(err.value)

    def test_get_headlines_directory_fails_with_wrong_name(self):
        """returns error when function is called with wrong pipeline name."""

        # Arrange
        # Act
        # Assert
        with pytest.raises(ValueError) as err:
            c.FileStorage.get_headlines_directory("wrong_name_dag")
        assert "No directory path for given pipeline name" in str(err.value)

    def test_get_csv_directory_fails_with_wrong_name(self):
        """returns error when function is called with wrong pipeline name."""

        # Arrange
        # Act
        # Assert
        with pytest.raises(ValueError) as err:
            c.FileStorage.get_csv_directory("wrong_name_dag")
        assert "No directory path for given pipeline name" in str(err.value)


@pytest.mark.networktests
class TestNetworkOperations:
    """tests news retrieval functions doing remote calls to the News APIs."""

    @patch('requests.Response', autospec=True)
    def test_get_news_http_call_success(self, response_obj):
        """returns response object has a valid 200 OK response-status code."""

        # Arrange
        # response object returns an OK status code
        response_obj.status_code = requests.codes.ok
        # configure call to the Response object's json() to return dummy data
        response_obj.json.side_effect = lambda: {"key": "value"}
        # configure Response object 'encoding' attribute
        response_obj.encoding = "utf-8"
        # retrieve the path to the folder the json file is saved to
        path = c.FileStorage.get_news_directory("tempus_challenge_dag")

        # setup a fake environment variable. real code gets it via Python's
        # os.environ() or Airflow's Variable.get() to get the name of the
        # current pipeline.
        os_environ_variable = "tempus_challenge_dag"

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory to test the method
            patcher.fs.create_dir(path)

        # Act
            result = c.NetworkOperations.get_news(response_obj,
                                                  news_dir=path,
                                                  gb_var=os_environ_variable)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert result[0] is True

    @patch('requests.get', autospect=True)
    def test_get_source_headlines_call_http_successfully(self, request_method):
        """a remote call to retrieve a source's top-headlines succeeds"""

        # Arrange
        # setup a dummy URL resembling the http call to get top-headlines
        base_url = "https://newsapi.org/v2"
        endpoint = "top-headlines?"
        params = "sources=abc-news"
        id_source = params.split("=")[1]
        key = env.NEWS_API_KEY

        # craft http request
        header = "/".join([base_url, endpoint])
        http_call = "".join([header, params])
        api_key = "apiKey=" + env.NEWS_API_KEY
        http_call_with_key = "&".join([http_call, api_key])

        # Act
        c.NetworkOperations.get_source_headlines(id_source,
                                                 header,
                                                 request_method,
                                                 key)

        # Assert
        request_method.assert_called_with(http_call_with_key)

    @patch('requests.get', autospect=True)
    def test_get_source_headlines_returns_successfully(self, request_method):
        """call to retrieve a source top-headlines makes http call correctly"""

        # Arrange
        # craft the kind of expected http response when the method is called
        response_obj = MagicMock(spec=requests.Response)
        response_obj.status_code = requests.codes.ok
        request_method.side_effect = lambda url: response_obj

        # setup a dummy URL resembling the http call to get top-headlines
        base_url = "https://newsapi.org/v2"
        endpoint = "top-headlines?"
        params = "sources=abc-news"
        id_source = params.split("=")[1]
        header = "/".join([base_url, endpoint])
        key = env.NEWS_API_KEY

        # Act
        result = c.NetworkOperations.get_source_headlines(id_source,
                                                          header,
                                                          request_method,
                                                          key)
        # Assert
        assert result[1] == requests.codes.ok

    @patch('requests.get', autospec=True)
    def test_get_source_headlines_http_call_fails(self, request_method):
        """news api call to retrieve top-headlines fails"""

        # Arrange
        # craft the kind of expected http response when the method is called
        response_obj = MagicMock(spec=requests.Response)
        response_obj.status_code = requests.codes.bad
        request_method.side_effect = lambda url: response_obj

        # setup a dummy URL resembling the http call to get top-headlines
        base_url = "https://newsapi.org/v2"
        endpoint = "top-headlines?"
        params = "sources=abc-news"
        id_source = params.split("=")[1]
        header = "/".join([base_url, endpoint])
        key = env.NEWS_API_KEY

        # Act
        result = c.NetworkOperations.get_source_headlines(id_source,
                                                          header,
                                                          request_method,
                                                          key)
        # Assert
        assert result[1] == requests.codes.bad_request

    @patch('requests.get', autospec=True)
    def test_get_source_headlines_no_source_fails(self, request_method):
        """call to retrieve source headlines with no-source fails"""

        # Arrange
        # setup a dummy URL resembling the http call to get top-headlines
        base_url = "https://newsapi.org/v2"
        endpoint = "top-headlines?"
        id_source = None
        header = "/".join([base_url, endpoint])
        key = env.NEWS_API_KEY

        # Act
        with pytest.raises(ValueError) as err:
            c.NetworkOperations.get_source_headlines(id_source,
                                                     header,
                                                     request_method,
                                                     key)

        # Assert
        expected_message = str(err.value)
        assert "'source_id' cannot be left blank" in expected_message

    @patch('requests.get')
    def test_get_source_headlines_no_api_key_fails(self, request_method):
        """call to retrieve source headlines with no api key fails"""

        # Arrange
        # setup a dummy URL resembling the http call to get top-headlines
        base_url = "https://newsapi.org/v2"
        endpoint = "top-headlines?"
        params = "sources=abc-news"
        id_source = params.split("=")[1]
        header = "/".join([base_url, endpoint])

        # Act
        with pytest.raises(ValueError) as err:
            c.NetworkOperations.get_source_headlines(id_source,
                                                     header,
                                                     request_method)

        # Assert
        expected_message = str(err.value)
        assert "No News API Key found" in expected_message


@pytest.mark.extractiontests
class TestExtractOperations:
    """tests the functions for the task to extract headlines from data."""

    @pytest.mark.skip
    def test_get_headlines(self):
        """test the retrieval of the top headlines."""

        # Test Cases Needed:
        #       What does 'get_headlines' do really ?:
        #
        # get context-specific news directory (get_news_directory)
        #
        # for each file in that directory
        #   read the file (json load) in (get_headlines)
        #   get the news sources id and put them in a list (extract source-id)
        #
        # for each id
        #   make an http call to get each headlines as json (get_headlines_api)
        #   extract the headlines and put them into a json (extract_headlines)
        #   write the json to the 'headlines' directory (write_to_json)
        pass

    def test_extract_news_source_id_succeeds(self):
        """extracting the 'id' parameter of sources in a json file succeeds."""

        # parse the news json 'sources' tag for all 'id' tags

        # Arrange
        # create some dummy json resembling the valid news json data
        dummy_data = {"status": "ok", "sources": [
                     {
                      "id": "abc-news",
                      "name": "ABC News",
                      "description":
                      "Your trusted source for breaking news, analysis, exclusive \
                      interviews, headlines, and videos at ABCNews.com.",
                      "url": "https://abcnews.go.com",
                      "category": "general",
                      "language": "en",
                      "country": "us"},
                     {"id": "abc-news-au",
                      "name": "ABC News (AU)",
                      "description": "Australia's most trusted source of local, \
                      national and world news. Comprehensive, independent, \
                      in-depth analysis, the latest business, sport, weather \
                      and more.",
                      "url": "http://www.abc.net.au/news",
                      "category": "general",
                      "language": "en",
                      "country": "au"}]
                      }

        # Act
        result = c.ExtractOperations.extract_news_source_id(dummy_data)

        # Assert
        expected_ids = ["abc-news", "abc-news-au"]
        assert expected_ids == result[0]

    def test_extract_headlines_succeeds(self):
        """return successful extraction of headlines from json data."""

        # parse the json headlines data for all 'title' tags

        # Arrange
        # create some dummy json resembling the valid news headlines json data
        dummy_data = {"status": "ok",
                      "totalResults": 20,
                      "articles":
                      [{"source": {
                        "id": "null",
                        "name": "Espn.com"},
                        "author": "null",
                        "title": "Odell Beckham Jr. walks into locker room",
                        "description": "Odell Beckham Jr. walked off field.",
                        "url": "null",
                        "urlToImage": "null",
                        "publishedAt": "2018-10-12T04:18:45Z",
                        "content": "EAST RUTHERFORD, N.J."}]}

        # Act
        result = c.ExtractOperations.extract_news_headlines(dummy_data)

        # Assert
        expected_headlines = ["Odell Beckham Jr. walks into locker room"]
        assert result == expected_headlines

    def test_extract_news_source_id_no_sources_fails(self):
        """no source tag in the json data fails the extraction process."""

        # Arrange
        # create some dummy json resembling the news json data with no
        # source tag.
        dummy_data = {"status": "ok"}

        # Act
        with pytest.raises(KeyError) as err:
            c.ExtractOperations.extract_news_source_id(dummy_data)

        # Assert
        expected_message = str(err.value)

        assert "news json has no 'sources' data" in expected_message

    def test_extract_news_source_id_empty_sources_fails(self):
        """empty source tag in the json data fails the extraction process."""

        # Arrange
        # create some dummy json resembling the news json data with an empty
        # source tag.
        dummy_data = {"status": "ok", "sources": []}

        # Act
        with pytest.raises(ValueError) as err:
            c.ExtractOperations.extract_news_source_id(dummy_data)

        # Assert
        expected_message = str(err.value)

        assert "'sources' tag in json is empty" in expected_message

    def test_extract_headlines_no_news_article_fails(self):
        """extraction of headlines with no article tag fails."""

        # Arrange
        # create some dummy json resembling the news json headline data with no
        # article tag.
        dummy_data = {"status": "ok", "totalResults": 20}

        # Act
        with pytest.raises(KeyError) as err:
            c.ExtractOperations.extract_news_headlines(dummy_data)

        # Assert
        expected_message = str(err.value)

        assert "news json has no 'articles' data" in expected_message

    def test_extract_headlines_empty_news_article_fails(self):
        """return successful extraction of headlines from json."""

        # Arrange
        # create some dummy json resembling the valid news headlines json data
        dummy_data = {"status": "ok",
                      "totalResults": 20,
                      "articles": []}

        # Act
        with pytest.raises(ValueError) as err:
            c.ExtractOperations.extract_news_headlines(dummy_data)

        # Assert
        expected_message = str(err.value)

        assert "'articles' tag in json is empty" in expected_message


@pytest.mark.transformtests
class TestTransformOperations:
    """test the functions for task to transform json headlines to csv."""

    @pytest.mark.skip
    def test_flatten_to_csv(self):
        """test the function to flatten a json to csv."""
        pass


@pytest.mark.uploadtests
class TestUploadOperations:
    """test the functions for task to upload csvs to Amazon S3."""

    @pytest.mark.skip
    def test_upload_csv_to_s3(self):
        """test the uploading of csvs to an s3 location."""
        pass

    # Loop of extract-transform-load. test_process_data
    @pytest.mark.skip
    def test_source_headlines(self):
        """test the flattening of csvs and their s3 upload for each source."""
        pass
