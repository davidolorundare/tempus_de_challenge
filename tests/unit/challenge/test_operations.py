"""Tempus challenge  - Unit Tests.

Defines unit tests for underlining functions to operators of tasks in the DAGs.
"""

import boto3
import datetime
import json
import pandas as pd
import pandas
import os
import pytest
import requests

from unittest.mock import MagicMock
from unittest.mock import patch

from airflow.models import DAG

from dags import challenge as c
from dags import config

from pyfakefs.fake_filesystem_unittest import Patcher


class MissingApiKeyError(ValueError):
    """raised when no api key is found or set"""
    pass


class NoFilesFoundError(FileNotFoundError):
    """raised no files of a particular type exist in a directory"""
    pass


# @pytest.mark.storagetests
# class TestFileStorage:
#     """Tests the creation of the tempoary datastores used during ETL tasks.

#     Maybe mock and test that os.path.exists(directory_path) is False before
#     the call and True afterwards.
#     test if directory already exists after the call. VERY IMPORTANT!
#     """

#     @pytest.fixture(scope='class')
#     def home_directory_res(self) -> str:
#         """returns a pytest resource - path to the Airflow Home directory."""
#         return str(os.environ['HOME'])

#     @pytest.fixture(scope='class')
#     def data_directories_res(self) -> list:
#         """returns a pytest resouce: list of data directories names."""
#         return ['news', 'headlines', 'csv']

#     @pytest.fixture(scope='class')
#     def airflow_context(self) -> dict:
#         """returns an airflow context object for tempus_challenge_dag.

#         Mimics parts of the airflow context returned during execution
#         of the tempus_challenge_dag.

#         https://airflow.apache.org/code.html#default-variables
#         """

#         dag = MagicMock(spec=DAG)
#         dag.dag_id = "tempus_challenge_dag"

#         return {
#             'ds': datetime.datetime.now().isoformat().split('T')[0],
#             'dag': dag
#         }

#     @pytest.fixture(scope='class')
#     def airflow_context_bonus(self) -> dict:
#         """return an airflow context object for tempus_bonus_challange_dag.

#         Mimics parts of the airflow context returned during execution
#         of the tempus_bonus_challenge_dag.

#         https://airflow.apache.org/code.html#default-variables
#         """

#         dag = MagicMock(spec=DAG)
#         dag.dag_id = "tempus_bonus_challenge_dag"

#         return {
#             'ds': datetime.datetime.now().isoformat().split('T')[0],
#             'dag': dag
#         }

#     @patch('os.makedirs', autospec=True)
#     @patch('os.path.join', autospec=True)
#     def test_create_news_data_store_pipe1_success(self,
#                                                   mock_path_func,
#                                                   mock_dir_func,
#                                                   home_directory_res,
#                                                   data_directories_res,
#                                                   airflow_context):
#         """call to create the tempoary 'news' datastore folders used by the
#         tempus_challenge_dag operators succeeds.
#         """

#         # Arrange

#         # Act
#         c.FileStorage.create_data_stores("news",
#                                          mock_path_func,
#                                          mock_dir_func,
#                                          **airflow_context)

#         # Assert
#         mock_path_func.assert_called_with(home_directory_res,
#                                           'tempdata',
#                                           'tempus_challenge_dag',
#                                           data_directories_res[0])
#         mock_path_func.reset_mock()
#         mock_dir_func.assert_called_with(mock_path_func(
#                                          home_directory_res,
#                                          'tempdata',
#                                          'tempus_challenge_dag',
#                                          data_directories_res[0]),
#                                          exist_ok=True)

#     @patch('os.makedirs', autospec=True)
#     @patch('os.path.join', autospec=True)
#     def test_create_headlines_data_store_pipe1_success(self,
#                                                        mock_path_func,
#                                                        mock_dir_func,
#                                                        home_directory_res,
#                                                        data_directories_res,
#                                                        airflow_context):
#         """call to create the tempoary 'headlines' datastore folders used by the
#         tempus_challenge_dag operators succeed.
#         """

#         # Arrange

#         # Act
#         c.FileStorage.create_data_stores("headlines",
#                                          mock_path_func,
#                                          mock_dir_func,
#                                          **airflow_context)

#         # Assert
#         mock_path_func.assert_called_with(home_directory_res,
#                                           'tempdata',
#                                           'tempus_challenge_dag',
#                                           data_directories_res[1])
#         mock_path_func.reset_mock()
#         mock_dir_func.assert_called_with(mock_path_func(
#                                          home_directory_res,
#                                          'tempdata',
#                                          'tempus_challenge_dag',
#                                          data_directories_res[1]),
#                                          exist_ok=True)

#     @patch('os.makedirs', autospec=True)
#     @patch('os.path.join', autospec=True)
#     def test_create_csv_data_store_pipe1_success(self,
#                                                  mock_path_func,
#                                                  mock_dir_func,
#                                                  home_directory_res,
#                                                  data_directories_res,
#                                                  airflow_context):
#         """call to create the tempoary 'csv' datastore folders used by the
#         tempus_challenge_dag operators.
#         """

#         # Arrange

#         # Act
#         c.FileStorage.create_data_stores("csv",
#                                          mock_path_func,
#                                          mock_dir_func,
#                                          **airflow_context)

#         # Assert
#         mock_path_func.assert_called_with(home_directory_res,
#                                           'tempdata',
#                                           'tempus_challenge_dag',
#                                           data_directories_res[2])
#         mock_path_func.reset_mock()
#         mock_dir_func.assert_called_with(mock_path_func(
#                                          home_directory_res,
#                                          'tempdata',
#                                          'tempus_challenge_dag',
#                                          data_directories_res[2]),
#                                          exist_ok=True)

#     @patch('os.makedirs', autospec=True)
#     @patch('os.path.join', autospec=True)
#     def test_create_news_data_store_pipe2_success(self,
#                                                   mock_path_func,
#                                                   mock_dir_func,
#                                                   home_directory_res,
#                                                   data_directories_res,
#                                                   airflow_context_bonus):
#         """call to create the tempoary 'news' datastore folders used by the
#         tempus_bonus_challenge_dag operators succeeds.
#         """

#         # Arrange

#         # Act
#         c.FileStorage.create_data_stores("news",
#                                          mock_path_func,
#                                          mock_dir_func,
#                                          **airflow_context_bonus)
#         # Assert
#         mock_path_func.assert_called_with(home_directory_res,
#                                           'tempdata',
#                                           'tempus_bonus_challenge_dag',
#                                           data_directories_res[0])
#         mock_path_func.reset_mock()
#         mock_dir_func.assert_called_with(mock_path_func(
#                                          home_directory_res,
#                                          'tempdata',
#                                          'tempus_bonus_challenge_dag',
#                                          data_directories_res[0]),
#                                          exist_ok=True)

#     @patch('os.makedirs', autospec=True)
#     @patch('os.path.join', autospec=True)
#     def test_create_headlines_data_store_pipe2_success(self,
#                                                        mock_path_func,
#                                                        mock_dir_func,
#                                                        home_directory_res,
#                                                        data_directories_res,
#                                                        airflow_context_bonus):
#         """call to create the tempoary 'headlines' datastore folders used by the
#         tempus_bonus_challenge_dag operators succeeds.
#         """

#         # Arrange

#         # Act
#         c.FileStorage.create_data_stores("headlines",
#                                          mock_path_func,
#                                          mock_dir_func,
#                                          **airflow_context_bonus)
#         # Assert
#         mock_path_func.assert_called_with(home_directory_res,
#                                           'tempdata',
#                                           'tempus_bonus_challenge_dag',
#                                           data_directories_res[1])
#         mock_path_func.reset_mock()
#         mock_dir_func.assert_called_with(mock_path_func(
#                                          home_directory_res,
#                                          'tempdata',
#                                          'tempus_bonus_challenge_dag',
#                                          data_directories_res[1]),
#                                          exist_ok=True)

#     @patch('os.makedirs', autospec=True)
#     @patch('os.path.join', autospec=True)
#     def test_create_csv_data_store_pipe2_success(self,
#                                                  mock_path_func,
#                                                  mock_dir_func,
#                                                  home_directory_res,
#                                                  data_directories_res,
#                                                  airflow_context_bonus):
#         """call to create the tempoary 'csv' datastore folders used by the
#         tempus_bonus_challenge_dag operators succeeds.
#         """

#         # Arrange

#         # Act
#         c.FileStorage.create_data_stores("csv",
#                                          mock_path_func,
#                                          mock_dir_func,
#                                          **airflow_context_bonus)
#         # Assert
#         mock_path_func.assert_called_with(home_directory_res,
#                                           'tempdata',
#                                           'tempus_bonus_challenge_dag',
#                                           data_directories_res[2])
#         mock_path_func.reset_mock()
#         mock_dir_func.assert_called_with(mock_path_func(
#                                          home_directory_res,
#                                          'tempdata',
#                                          'tempus_bonus_challenge_dag',
#                                          data_directories_res[2]),
#                                          exist_ok=True)

#     def test_get_news_dir_returns_correct_path(self, home_directory_res):
#         """returns correct news path when called correctly with pipeline name.
#         """

#         # Arrange
#         news_path = os.path.join(home_directory_res,
#                                  'tempdata',
#                                  'tempus_challenge_dag',
#                                  'news')

#         # Act
#         path = c.FileStorage.get_news_directory("tempus_challenge_dag")

#         # Assert
#         assert path == news_path

#     def test_get_headlines_dir_returns_correct_path(self, home_directory_res):
#         """returns correct headlines path when called correctly with DAG name.
#         """

#         # Arrange
#         news_path = os.path.join(home_directory_res,
#                                  'tempdata',
#                                  'tempus_bonus_challenge_dag',
#                                  'headlines')

#         pipeline = "tempus_bonus_challenge_dag"

#         # Act
#         path = c.FileStorage.get_headlines_directory(pipeline)

#         # Assert
#         assert path == news_path

#     def test_get_csv_dir_returns_correct_path(self, home_directory_res):
#         """returns correct csv path when called correctly with pipeline name.
#         """

#         # Arrange
#         news_path = os.path.join(home_directory_res,
#                                  'tempdata',
#                                  'tempus_challenge_dag',
#                                  'csv')

#         # Act
#         path = c.FileStorage.get_csv_directory("tempus_challenge_dag")

#         # Assert
#         assert path == news_path

#     def test_write_json_to_file_succeeds(self):
#         """successful write of json string data to a file directory."""

#         # Arrange
#         # dummy json data - Python object converted to json string
#         json_data = json.dumps({'key': 'value'})
#         # path to a directory that doesn't yet exist
#         datastore_folder_path = "/data/"
#         # detect presence of a file in the directory before and after
#         # calling the method under test.
#         file_is_absent = False
#         file_is_present = False

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory to test the method
#             patcher.fs.create_dir(datastore_folder_path)

#             # ensure that the newly created directory is really empty
#             if not os.listdir(datastore_folder_path):
#                 file_is_absent = True

#             # Act
#             # write some dummy json data into a file a save to that directory
#             result = c.FileStorage.write_json_to_file(json_data,
#                                                       datastore_folder_path,
#                                                       filename="test")
#             # inspect the directory - the json file should now be in there
#             if os.listdir(datastore_folder_path):
#                 file_is_present = True

#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert file_is_absent is True
#         assert result is True
#         assert file_is_present is True

#     def test_write_source_headlines_to_file_no_argument_fails(self):
#         """writes of news source headlines to a directory fails with missing
#         parameter.
#         """

#         # Arrange
#         key = None
#         ids = ['abc-news-au', 'bbc-news']
#         names = ['ABCNews', 'BBCNews']
#         hd_dir = '/tempdata/data'

#         # Act
#         with pytest.raises(ValueError) as err:
#             c.FileStorage.write_source_headlines_to_file(ids,
#                                                          names,
#                                                          hd_dir,
#                                                          key)

#         # Assert
#         actual_message = str(err.value)
#         assert "is blank" in actual_message

#     def test_write_json_to_file_fails_with_wrong_directory_path(self):
#         """write of json data to a file to a non-existent directory
#         fails correctly.
#         """

#         # Arrange
#         # valid dummy json data
#         json_data = json.dumps({'some_key': 'some_value'})

#         # path to directory that doesn't yet exist
#         datastore_folder_path = "/data/"

#         # Act
#         # Assert
#         # writing some dummy json data into a file to save to that non-existent
#         # directory should throw errors
#         with pytest.raises(OSError) as err:
#             c.FileStorage.write_json_to_file(json_data,
#                                              datastore_folder_path,
#                                              filename="test")
#         expected = "Directory {} does not exist".format(datastore_folder_path)
#         assert expected in str(err.value)

#     def test_write_json_to_file_fails_with_bad_data(self):
#         """write of a invalid json data to a file (already existent directory
#         fails correctly.
#         """

#         # Arrange
#         # invalid or bad json data
#         # json_data = {u"my_key": u'\uda00'}  # int('-inf')

#         # path to directory that doesn't yet exist but will be created
#         datastore_folder_path = "/data/"

#         # detect presence of a file in the directory before and after
#         # calling the method under test.
#         file_is_absent = False
#         file_is_present = False

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory to test the method
#             patcher.fs.create_dir(datastore_folder_path)

#             # ensure that the newly created directory is really empty
#             if not os.listdir(datastore_folder_path):
#                 file_is_absent = True

#         # Act
#             # writing some dummy bad json data into a file to save to that
#             # existent directory should throw bad-input errors
#             with pytest.raises(ValueError) as err:
#                 c.FileStorage.write_json_to_file(datastore_folder_path,
#                                                  filename="test",
#                                                  data=int('w'))

#             actual_error = str(err.value)

#             # inspect the directory - the method failed, the json file should
#             # not be in there and file_is_present should remain False
#             if os.listdir(datastore_folder_path):
#                 file_is_present = True

#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert file_is_absent is True
#         assert file_is_present is False
#         assert "invalid literal" in actual_error

#     def test_get_news_directory_fails_with_wrong_name(self):
#         """returns error when function is called with wrong pipeline name."""

#         # Arrange
#         # Act
#         # Assert
#         with pytest.raises(ValueError) as err:
#             c.FileStorage.get_news_directory("wrong_name_dag")
#         assert "No directory path for given pipeline name" in str(err.value)

#     def test_get_headlines_directory_fails_with_wrong_name(self):
#         """returns error when function is called with wrong pipeline name."""

#         # Arrange
#         # Act
#         # Assert
#         with pytest.raises(ValueError) as err:
#             c.FileStorage.get_headlines_directory("wrong_name_dag")
#         assert "No directory path for given pipeline name" in str(err.value)

#     def test_get_csv_directory_fails_with_wrong_name(self):
#         """returns error when function is called with wrong pipeline name."""

#         # Arrange
#         # Act
#         # Assert
#         with pytest.raises(ValueError) as err:
#             c.FileStorage.get_csv_directory("wrong_name_dag")
#         assert "No directory path for given pipeline name" in str(err.value)


# @pytest.mark.networktests
# class TestNetworkOperations:
#     """tests news retrieval functions doing remote calls to the News APIs."""

#     @patch('requests.Response', autospec=True)
#     def test_get_news_http_call_success(self, response_obj):
#         """returns response object has a valid 200 OK response-status code."""

#         # Arrange
#         # response object returns an OK status code
#         response_obj.status_code = requests.codes.ok
#         # configure call to the Response object's json() to return dummy data
#         response_obj.json.side_effect = lambda: {"key": "value"}
#         # configure Response object 'encoding' attribute
#         response_obj.encoding = "utf-8"
#         # retrieve the path to the folder the json file is saved to
#         path = c.FileStorage.get_news_directory("tempus_challenge_dag")

#         # setup a fake environment variable. real code gets it via Python's
#         # os.environ() or Airflow's Variable.get() to get the name of the
#         # current pipeline.
#         os_environ_variable = "tempus_challenge_dag"

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory to test the method
#             patcher.fs.create_dir(path)

#         # Act
#             result = c.NetworkOperations.get_news(response_obj,
#                                                   news_dir=path,
#                                                   gb_var=os_environ_variable)

#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert result[0] is True

#     @patch('requests.PreparedRequest', autospec=True)
#     @patch('requests.Response', autospec=True)
#     def test_get_news_keyword_headlines_succeeds(self, response, request):
#         """call to the function returns successfully"""

#         # Arrange

#         # create function aliases
#         keyword_headline_func = c.NetworkOperations.get_news_keyword_headlines
#         storage_headline_dir_func = c.FileStorage.get_headlines_directory
#         storage_news_dir_func = c.FileStorage.get_news_directory

#         # response object returns an OK status code
#         response.status_code = requests.codes.ok

#         # configure call to the Response object's json() to return dummy data
#         response.json.side_effect = lambda: {"headline":
#                                              "Tempus solves Cancer"}

#         # configure Response object 'encoding' attribute
#         response.encoding = "utf-8"

#         # configure Response object's request parameters be a dummy url
#         cancer_url = "https://newsapi.org/v2/top-headlines?q=cancer&apiKey=543"
#         request.path_url = "/v2/top-headlines?q=cancer&apiKey=543"
#         request.url = cancer_url
#         response.request = request

#         # retrieve the path to the folder the json file is saved to
#         path_headline = storage_headline_dir_func("tempus_bonus_challenge_dag")
#         path_news = storage_news_dir_func("tempus_bonus_challenge_dag")

#         # filename of the keyword headline json file that will be created
#         fname = "cancer_headlines"

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory to test the method
#             patcher.fs.create_dir(path_headline)
#             patcher.fs.create_dir(path_news)

#         # Act
#             result = keyword_headline_func(response,
#                                            headlines_dir=path_headline,
#                                            filename=fname)

#             # return to the real filesystem and clear pyfakefs resources
#             patcher.tearDown()

#         # Assert
#         assert result is True

#     @patch('requests.get', autospect=True)
#     def test_get_source_headlines_call_http_successfully(self, request_method):
#         """a remote call to retrieve a source's top-headlines succeeds"""

#         # Arrange
#         # setup a dummy URL resembling the http call to get top-headlines
#         base_url = "https://newsapi.org/v2"
#         endpoint = "top-headlines?"
#         params = "sources=abc-news"
#         id_source = params.split("=")[1]
#         key = config.NEWS_API_KEY

#         # craft http request
#         header = "/".join([base_url, endpoint])
#         http_call = "".join([header, params])
#         api_key = "apiKey=" + config.NEWS_API_KEY
#         http_call_with_key = "&".join([http_call, api_key])

#         # Act
#         c.NetworkOperations.get_source_headlines(id_source,
#                                                  header,
#                                                  request_method,
#                                                  key)

#         # Assert
#         request_method.assert_called_with(http_call_with_key)

#     @patch('requests.get', autospect=True)
#     def test_get_source_headlines_returns_successfully(self, request_method):
#         """call to retrieve a source top-headlines makes http call correctly"""

#         # Arrange
#         # craft the kind of expected http response when the method is called
#         response_obj = MagicMock(spec=requests.Response)
#         response_obj.status_code = requests.codes.ok
#         request_method.side_effect = lambda url: response_obj

#         # setup a dummy URL resembling the http call to get top-headlines
#         base_url = "https://newsapi.org/v2"
#         endpoint = "top-headlines?"
#         params = "sources=abc-news"
#         id_source = params.split("=")[1]
#         header = "/".join([base_url, endpoint])
#         key = config.NEWS_API_KEY

#         # Act
#         result = c.NetworkOperations.get_source_headlines(id_source,
#                                                           header,
#                                                           request_method,
#                                                           key)
#         # Assert
#         assert result.status_code == requests.codes.ok

#     @patch('requests.PreparedRequest', autospec=True)
#     @patch('requests.Response', autospec=True)
#     def test_get_news_keyword_headlines_fails_with_bad_request(self,
#                                                                response,
#                                                                request):
#         """call to the function fails whenw a Response in which there
#         was no query in the http request is passed"""

#         # Arrange

#         # create function aliases
#         keyword_headline_func = c.NetworkOperations.get_news_keyword_headlines
#         storage_headline_dir_func = c.FileStorage.get_headlines_directory

#         # response object returns an OK status code
#         response.status_code = requests.codes.ok

#         # configure call to the Response object's json() to return dummy data
#         response.json.side_effect = lambda: {"headline":
#                                              "Tempus can solve Cancer"}

#         # configure Response object 'encoding' attribute
#         response.encoding = "utf-8"

#         # configure Response object's request parameters be a dummy url
#         cancer_url = "https://newsapi.org/v2/top-headlines?apiKey=543"
#         request.path_url = "/v2/top-headlines?apiKey=543"
#         request.url = cancer_url
#         response.request = request

#         # retrieve the path to the folder the json file is saved to
#         path = storage_headline_dir_func("tempus_bonus_challenge_dag")

#         # filename of the keyword headline json file that will be created
#         fname = "cancer_headlines"

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory to test the method
#             patcher.fs.create_dir(path)

#         # Act
#             with pytest.raises(KeyError) as err:
#                 keyword_headline_func(response,
#                                       headlines_dir=path,
#                                       filename=fname)

#             # return to the real filesystem and clear pyfakefs resources
#             patcher.tearDown()

#         # Assert
#         actual_message = str(err.value)
#         assert "Query param not found" in actual_message

#     @patch('requests.get', autospec=True)
#     def test_get_source_headlines_http_call_fails(self, request_method):
#         """news api call to retrieve top-headlines fails"""

#         # Arrange
#         # craft the kind of expected http response when the method is called
#         response_obj = MagicMock(spec=requests.Response)
#         response_obj.status_code = requests.codes.bad
#         request_method.side_effect = lambda url: response_obj

#         # setup a dummy URL resembling the http call to get top-headlines
#         base_url = "https://newsapi.org/v2"
#         endpoint = "top-headlines?"
#         params = "sources=abc-news"
#         id_source = params.split("=")[1]
#         header = "/".join([base_url, endpoint])
#         key = config.NEWS_API_KEY

#         # Act
#         result = c.NetworkOperations.get_source_headlines(id_source,
#                                                           header,
#                                                           request_method,
#                                                           key)
#         # Assert
#         assert result.status_code == requests.codes.bad_request

#     @patch('requests.get', autospec=True)
#     def test_get_source_headlines_no_source_fails(self, request_method):
#         """call to retrieve source headlines with no-source fails"""

#         # Arrange
#         # setup a dummy URL resembling the http call to get top-headlines
#         base_url = "https://newsapi.org/v2"
#         endpoint = "top-headlines?"
#         id_source = None
#         header = "/".join([base_url, endpoint])
#         key = config.NEWS_API_KEY

#         # Act
#         with pytest.raises(ValueError) as err:
#             c.NetworkOperations.get_source_headlines(id_source,
#                                                      header,
#                                                      request_method,
#                                                      key)

#         # Assert
#         actual_message = str(err.value)
#         assert "'source_id' cannot be left blank" in actual_message

#     @patch('requests.get')
#     def test_get_source_headlines_no_api_key_fails(self, request_method):
#         """call to retrieve source headlines with no api key fails"""

#         # Arrange
#         # setup a dummy URL resembling the http call to get top-headlines
#         base_url = "https://newsapi.org/v2"
#         endpoint = "top-headlines?"
#         params = "sources=abc-news"
#         id_source = params.split("=")[1]
#         header = "/".join([base_url, endpoint])

#         # Act
#         with pytest.raises(ValueError) as err:
#             c.NetworkOperations.get_source_headlines(id_source,
#                                                      header,
#                                                      request_method)

#         # Assert
#         actual_message = str(err.value)
#         assert "No News API Key found" in actual_message


# @pytest.mark.extractiontests
# class TestExtractOperations:
#     """tests the functions for the task to extract headlines from data."""

#     @pytest.fixture(scope='class')
#     def home_directory_res(self) -> str:
#         """returns a pytest resource - path to the Airflow Home directory."""
#         return str(os.environ['HOME'])

#     @patch('requests.PreparedRequest', autospec=True)
#     @patch('requests.Response', autospec=True)
#     def test_extract_headline_keyword_success(self, response_obj, request_obj):
#         """successfully parse of request url returns keyword parameter."""

#         # Arrange
#         # response object returns an OK status code
#         response_obj.status_code = requests.codes.ok

#         # configure Response object's request parameters be a dummy url
#         cancer_url = "https://newsapi.org/v2/top-headlines?q=cancer&apiKey=543"
#         request_obj.path_url = "/v2/top-headlines?q=cancer&apiKey=543"
#         request_obj.url = cancer_url

#         response_obj.request = request_obj

#         # Act
#         result = c.ExtractOperations.extract_headline_keyword(response_obj)

#         # Assert
#         # extracted keyword parameter from the url should be 'cancer'
#         assert result == "cancer"

#     def test_extract_news_source_id_succeeds(self):
#         """extracting the 'id' parameter of sources in a json file succeeds."""

#         # parse the news json 'sources' tag for all 'id' tags

#         # Arrange
#         # create some dummy json resembling the valid news json data
#         dummy_data = {"status": "ok", "sources": [
#                      {
#                       "id": "abc-news",
#                       "name": "ABC News",
#                       "description":
#                       "Your trusted source for breaking news, analysis, exclusive \
#                       interviews, headlines, and videos at ABCNews.com.",
#                       "url": "https://abcnews.go.com",
#                       "category": "general",
#                       "language": "en",
#                       "country": "us"},
#                      {"id": "abc-news-au",
#                       "name": "ABC News (AU)",
#                       "description": "Australia's most trusted source of local, \
#                       national and world news. Comprehensive, independent, \
#                       in-depth analysis, the latest business, sport, weather \
#                       and more.",
#                       "url": "http://www.abc.net.au/news",
#                       "category": "general",
#                       "language": "en",
#                       "country": "au"}]
#                       }

#         # Act
#         result = c.ExtractOperations.extract_news_source_id(dummy_data)

#         # Assert
#         expected_ids = ["abc-news", "abc-news-au"]
#         assert expected_ids == result[0]

#     def test_create_news_headlines_json_returns_correctly(self):
#         """return valid json news data with sources and headlines ."""

#         # Arrange
#         # create some dummy json resembling the final source and headline data
#         dummy_data_expected = {"source": {
#                                "id": "abc-news",
#                                "name": "ABC NEWS"},
#                                "headlines": ['top-headline1', 'top-headline2']}

#         source_id = "abc-news"
#         source_name = "ABC NEWS"
#         top_headlines = ['top-headline1', 'top-headline2']

#         # Act
#         result = c.ExtractOperations.create_top_headlines_json(source_id,
#                                                                source_name,
#                                                                top_headlines)

#         # Assert
#         assert result == dummy_data_expected

#     def test_extract_headlines_succeeds(self):
#         """return successful extraction of headlines from json data."""

#         # parse the json headlines data for all 'title' tags

#         # Arrange
#         # create some dummy json resembling the valid news headlines json data
#         dummy_data = {"status": "ok",
#                       "totalResults": 20,
#                       "articles":
#                       [{"source": {
#                         "id": "null",
#                         "name": "Espn.com"},
#                         "author": "null",
#                         "title": "Odell Beckham Jr. walks into locker room",
#                         "description": "Odell Beckham Jr. walked off field.",
#                         "url": "null",
#                         "urlToImage": "null",
#                         "publishedAt": "2018-10-12T04:18:45Z",
#                         "content": "EAST RUTHERFORD, N.J."}]}

#         # Act
#         result = c.ExtractOperations.extract_news_headlines(dummy_data)

#         # Assert
#         expected_headlines = ["Odell Beckham Jr. walks into locker room"]
#         assert result == expected_headlines

#     def test_extract_jsons_source_info_succeds(self, home_directory_res):
#         """list of news json successfully extracts source id and names."""

#         # Arrange
#         dummy_dta1 = {"status": "ok", "sources": [
#                      {
#                       "id": "polygon",
#                       "name": "Polygon",
#                       "description":
#                       "Your trusted source for breaking news, analysis, exclusive \
#                       interviews, games and much more.",
#                       "url": "https://www.polygon.com",
#                       "category": "general",
#                       "language": "en",
#                       "country": "us"}]
#                       }

#         dummy_dta2 = {"status": "ok", "sources": [
#                      {
#                       "id": "abc-news",
#                       "name": "ABC News",
#                       "description":
#                       "Your trusted source for breaking news, analysis, exclusive \
#                       interviews, headlines, and videos at ABCNews.com.",
#                       "url": "https://abcnews.go.com",
#                       "category": "general",
#                       "language": "en",
#                       "country": "us"},
#                      {"id": "abc-news-au",
#                       "name": "ABC News (AU)",
#                       "description": "Australia's most trusted source of local, \
#                       national and world news. Comprehensive, independent, \
#                       in-depth analysis, the latest business, sport, weather \
#                       and more.",
#                       "url": "http://www.abc.net.au/news",
#                       "category": "general",
#                       "language": "en",
#                       "country": "au"}]
#                       }

#         dummy_dta3 = {"status": "ok", "sources": [
#                      {
#                       "id": "bbc-news",
#                       "name": "BBC News",
#                       "description":
#                       "Tempus daily news, analysis, exclusive \
#                       interviews, headlines, and videos at BBCNews.com.",
#                       "url": "https://abcnews.go.com",
#                       "category": "local",
#                       "language": "en",
#                       "country": "ng"},
#                      {"id": "abc-news-au",
#                       "name": "BBC News (AU)",
#                       "description": "Finding things that are of local, \
#                       national and world news. Comprehensive, independent, \
#                       in-depth analysis, the latest business, sport, weather \
#                       and more.",
#                       "category": "general",
#                       "language": "en",
#                       "country": "au"}]
#                       }

#         js_one_data = json.dumps(dummy_dta1)
#         js_two_data = json.dumps(dummy_dta2)
#         js_thre_data = json.dumps(dummy_dta3)
#         extract_func_alias = c.ExtractOperations.extract_jsons_source_info

#         news_path = os.path.join(home_directory_res, 'tempdata', 'jsons')
#         js_dir = news_path
#         js_list = ['stuff1.json', 'stuff2.json', 'stuff3.json']

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create dummy files
#             full_file_path_one = os.path.join(js_dir, 'stuff1.json')
#             full_file_path_two = os.path.join(js_dir, 'stuff2.json')
#             full_file_path_three = os.path.join(js_dir, 'stuff3.json')

#             # create a fake filesystem directory to test the method
#             patcher.fs.create_dir(js_dir)
#             patcher.fs.create_file(full_file_path_one, contents=js_one_data)
#             patcher.fs.create_file(full_file_path_two, contents=js_two_data)
#             patcher.fs.create_file(full_file_path_three, contents=js_thre_data)

#         # Act
#             actual_result = extract_func_alias(js_list, js_dir)

#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         expected = (['bbc-news', 'abc-news-au'], ['bbc news', 'bbc news (au)'])
#         assert expected == actual_result

#     def test_extract_news_data_from_dataframe_succeeds(self):
#         """extraction of information from news dataframe succeeds."""

#         # Arrange

#         # craft the invalid dataframe data
#         data = pd.DataFrame()
#         total_results = [1]
#         status = ['ok']

#         source1 = {"source": {"id": "wired", "name": "Wired"},
#                    "author": "Klint Finley", "title": "Microsoft\
#                    Calls a Truce in the Linux\
#                     Patent Wars", "description": "The software giant, whose\
#                      former CEO once called\
#                     Linux a \\\"cancer,\\\" will let others use 60,000\
#                      patents for Linux-related open source pro\
#                      jects.", "url": "https://www.wired.com/story/mic\
#                      rosoft-calls-truce-in-linux-patent-wars/", "urlToImage": "https://med\
#                      ia.wired.com/photos/5bbe9c0f2b915f2dff96d6f4/191:100/pas\
#                      s/Satya-Microsoft-MichelleG.jpg", "publishedAt": "20\
#                      18-10-11T23:33:03Z", "content": "Microsoft is calling\
#                       for a truce in the patent war. This week the company\
#                        said it will allow more than 2,600 other companies,\
#                         including traditional rivals like Google and IBM,\
#                          to use the technology behind 60,000 Microsoft\
#                           patents for their own Linux related o… [+4435 chars]"}

#         articles = [source1]

#         data['status'] = status
#         data['totalResults'] = total_results
#         data['articles'] = articles

#         # Act
#         result = c.ExtractOperations.extract_news_data_from_dataframe(data)

#         # Assert
#         # we know the function's return value is a dictionary.
#         empty_dict = None
#         if not result:
#             empty_dict = True
#         else:
#             empty_dict = False
#         assert empty_dict is False

#     def test_extract_news_data_from_dataframe_no_articles_fails(self):
#         """extraction of information from news dataframe fails
#         if there are no news articles.
#         """

#         # Arrange

#         # craft the invalid dataframe data
#         data = pd.DataFrame()
#         total_results = [0, 0, 0]
#         status = ['ok', 'ok', 'ok']
#         articles = [None, None, None]

#         data['status'] = status
#         data['totalResults'] = total_results
#         data['articles'] = articles

#         # Act
#         result = c.ExtractOperations.extract_news_data_from_dataframe(data)

#         # Assert
#         # we know the function's return value is a dictionary.
#         empty_dict = None
#         if not result:
#             empty_dict = True
#         else:
#             empty_dict = False
#         assert empty_dict is True

#     def test_extract_jsons_source_info_no_data_fails(self, home_directory_res):
#         """list of news json fails at extracting source id and names with
#         bad data.
#         """

#         # Arrange
#         dummy_data_one = {"sources": {
#                           "id": "u'asd",
#                           "name": "u'erw"},
#                           "headlines": []}

#         dummy_data_two = {"sources": [
#                           {"id": 33435},
#                           {"nafme": 'ABC News2'}],
#                           "name": [
#                           {"ido": 3465635},
#                           {"name": 'ABC News2'}],
#                           "headlines": ['headlines1', 'headlines2']}

#         json_file_one_data = json.dumps(dummy_data_one)
#         json_file_two_data = json.dumps(dummy_data_two)
#         extract_func_alias = c.ExtractOperations.extract_jsons_source_info

#         news_path = os.path.join(home_directory_res, 'tempdata', 'jsons')
#         json_directory = news_path
#         json_list = ['stuff1.json', 'stuff2.json']

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create dummy files
#             full_file_path_one = os.path.join(json_directory, 'stuff1.json')
#             full_file_path_two = os.path.join(json_directory, 'stuff2.json')

#             # create a fake filesystem directory files to test the method
#             patcher.fs.create_dir(json_directory)
#             patcher.fs.create_file(full_file_path_one,
#                                    contents=json_file_one_data,
#                                    encoding="utf-16")
#             patcher.fs.create_file(full_file_path_two,
#                                    contents=json_file_two_data,
#                                    encoding="utf-16")

#         # Act
#             with pytest.raises(ValueError) as err:
#                 extract_func_alias(json_list, json_directory)

#             expected = str(err.value)
#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert "Parsing Error" in expected

#     def test_extract_news_source_id_no_sources_fails(self):
#         """no source tag in the json data fails the extraction process."""

#         # Arrange
#         # create some dummy json resembling the news json data with no
#         # source tag.
#         dummy_data = {"status": "ok"}

#         # Act
#         with pytest.raises(KeyError) as err:
#             c.ExtractOperations.extract_news_source_id(dummy_data)

#         # Assert
#         actual_message = str(err.value)

#         assert "news json has no 'sources' data" in actual_message

#     def test_extract_news_source_id_empty_sources_fails(self):
#         """empty source tag in the json data fails the extraction process."""

#         # Arrange
#         # create some dummy json resembling the news json data with an empty
#         # source tag.
#         dummy_data = {"status": "ok", "sources": []}

#         # Act
#         with pytest.raises(ValueError) as err:
#             c.ExtractOperations.extract_news_source_id(dummy_data)

#         # Assert
#         actual_message = str(err.value)

#         assert "'sources' tag in json is empty" in actual_message

#     def test_extract_headlines_no_news_article_fails(self):
#         """extraction of headlines with no article tag fails."""

#         # Arrange
#         # create some dummy json resembling the news json headline data with no
#         # article tag.
#         dummy_data = {"status": "ok", "totalResults": 20}

#         # Act
#         with pytest.raises(KeyError) as err:
#             c.ExtractOperations.extract_news_headlines(dummy_data)

#         # Assert
#         actual_message = str(err.value)

#         assert "news json has no 'articles' data" in actual_message

#     def test_extract_headlines_empty_news_article_returns_empty_list(self):
#         """return empty list when there is no headlines."""

#         # Arrange
#         # create some dummy json resembling the valid news headlines json data
#         dummy_data = {"status": "ok",
#                       "totalResults": 20,
#                       "articles": []}

#         # Act
#         result = c.ExtractOperations.extract_news_headlines(dummy_data)

#         # Assert
#         assert result == []

#     def test_create_news_headlines_json_no_source_id_fails(self):
#         """passing no source_id to the function raises errors."""

#         # Arrange
#         source_id = None
#         source_name = "ABC NEWS"
#         top_headlines = ['top-headline1', 'top-headline2']

#         # Act
#         with pytest.raises(ValueError) as err:
#             c.ExtractOperations.create_top_headlines_json(source_id,
#                                                           source_name,
#                                                           top_headlines)

#         # Assert
#         actual_message = str(err.value)
#         assert "'source_id' cannot be blank" in actual_message

#     def test_create_news_headlines_json_no_source_name_fails(self):
#         """passing no source_name to the function raises errors."""

#         # Arrange
#         source_id = "abc-news"
#         source_name = None
#         top_headlines = ['top-headline1', 'top-headline2']

#         # Act
#         with pytest.raises(ValueError) as err:
#             c.ExtractOperations.create_top_headlines_json(source_id,
#                                                           source_name,
#                                                           top_headlines)

#         # Assert
#         actual_message = str(err.value)
#         assert "'source_name' cannot be blank" in actual_message

#     def test_create_news_headlines_json_no_headlines_list_fails(self):
#         """passing no list of headlines to the function raises errors."""

#         # Arrange
#         source_id = "abc-news"
#         source_name = "ABC NEWS"
#         top_headlines = None

#         # Act
#         with pytest.raises(ValueError) as err:
#             c.ExtractOperations.create_top_headlines_json(source_id,
#                                                           source_name,
#                                                           top_headlines)

#         # Assert
#         actual_message = str(err.value)
#         assert "'headlines' cannot be blank" in actual_message

#     @patch('requests.PreparedRequest', autospec=True)
#     @patch('requests.Response', autospec=True)
#     def test_extract_headline_keyword_no_query_in_url_fails(self,
#                                                             response_obj,
#                                                             request_obj):
#         """request url with no query keyword parameter fails."""

#         # Arrange
#         # response object returns an OK status code
#         response_obj.status_code = requests.codes.ok

#         # configure Response object's request parameters be a dummy url
#         cancer_url = "https://newsapi.org/v2/top-headlines?apiKey=543"
#         request_obj.path_url = "/v2/top-headlines?apiKey=543"
#         request_obj.url = cancer_url

#         response_obj.request = request_obj

#         # Act
#         with pytest.raises(KeyError) as err:
#             c.ExtractOperations.extract_headline_keyword(response_obj)

#         # Assert
#         actual_message = str(err.value)
#         assert "Query param not found in URL" in actual_message


# @pytest.mark.transformtests
# class TestTransformOperations:
#     """test the functions for task to transform json headlines to csv."""

#     @pytest.fixture(scope='class')
#     def airflow_context(self) -> dict:
#         """returns an airflow context object for tempus_challenge_dag.

#         Mimics parts of the airflow context returned during execution
#         of the tempus_challenge_dag.

#         https://airflow.apache.org/code.html#default-variables
#         """

#         dag = MagicMock(spec=DAG)
#         dag.dag_id = "tempus_challenge_dag"
#         current_execution_time = str(datetime.datetime.now())

#         return {
#             'dag': dag,
#             'execution_date': current_execution_time
#         }

#     @pytest.fixture(scope='class')
#     def home_directory_res(self) -> str:
#         """returns a pytest resource - path to the Airflow Home directory."""
#         return str(os.environ['HOME'])

#     @pytest.fixture(scope='class')
#     def headline_dir_res(self) -> str:
#         """returns a pytest resource - path to the 'tempus_challenge_dag'
#         pipeline headlines directory.
#         """

#         headlines_path = os.path.join(self.home_directory_res(),
#                                       'tempdata',
#                                       'tempus_challenge_dag',
#                                       'headlines')
#         return headlines_path

#     @pytest.fixture(scope='class')
#     def csv_dir_res(self) -> str:
#         """returns a pytest resource - path to the 'tempus_challenge_dag'
#         pipeline csv directory.
#         """

#         csv_path = os.path.join(self.home_directory_res(),
#                                 'tempdata',
#                                 'tempus_challenge_dag',
#                                 'csv')
#         return csv_path

#     @patch('pandas.read_json', autospec=True)
#     def test_transform_key_headlines_to_csv_convert_sucess(self,
#                                                            file_reader_func,
#                                                            home_directory_res):
#         """call to flatten jsons in the tempus_bonus_challenge_dag headline
#         folder succeeds."""

#         # Arrange
#         # name of the pipeline under test
#         pipeline_name = "tempus_bonus_challenge_dag"

#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         tf_func = c.TransformOperations.transform_key_headlines_to_csv
#         extract_func = c.ExtractOperations.extract_news_data_from_dataframe

#         # create the dummy input data that will be passed to the function
#         # under test
#         dummy_json_file = None
#         transform_data_df = MagicMock(spec=pandas.DataFrame)

#         # setup a Mock of the extract and transform function dependencies
#         tf_func_mock = MagicMock(spec=tf_func)
#         extract_func_mock = MagicMock(spec=extract_func)

#         # define the mock function behaviors when called
#         extract_func_mock.side_effect = lambda data: "extracted data"
#         tf_func_mock.side_effect = lambda data: transform_data_df
#         file_reader_func.side_effect = lambda data: "read-in json file"

#         # path to the fake csv directory the function under test
#         # uses
#         csv_dir = os.path.join(home_directory_res,
#                                'tempdata',
#                                pipeline_name,
#                                'csv')

#         # name and path to the file that will be created after transformation
#         filename = str(datetime.datetime.now()) + "_" + "sample.csv"
#         # fp = os.path.join(csv_dir, filename)

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and place the dummy csv files
#             # in that directory to test the method
#             patcher.fs.create_dir(csv_dir)

#             # calling the transformed DataFrame's to_csv() creates a new
#             # csv file in the fake directory
#             transform_data_df.to_csv.side_effect = patcher.fs.create_file

#         # Act
#             result = tf_func(dummy_json_file,
#                              filename,
#                              extract_func_mock,
#                              tf_func_mock,
#                              file_reader_func)

#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         # return status of the operation should be True to indicate success
#         assert result is True

#     @patch('pandas.read_json', autospec=True)
#     def test_transform_jsons_to_dataframe_merger_succeeds(self,
#                                                           file_reader_func):
#         """merging a set of transformed DataFrames from jsons succeeds."""

#         # Arrange

#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         tf_func = c.TransformOperations.transform_jsons_to_dataframe_merger
#         extract_func = c.ExtractOperations.extract_news_data_from_dataframe
#         data_to_df_func = c.TransformOperations.transform_data_to_dataframe

#         # create the dummy input data that will be passed to the function
#         # under test
#         json_files = ['file1.json', 'file2.json']
#         data_df1 = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
#                                  'B': ['B0', 'B1', 'B2', 'B3'],
#                                  'C': ['C0', 'C1', 'C2', 'C3'],
#                                  'D': ['D0', 'D1', 'D2', 'D3']},
#                                 index=[0, 1, 2, 3])

#         data_df2 = pd.DataFrame({'A': ['A4', 'A5', 'A6', 'A7'],
#                                  'B': ['B4', 'B5', 'B6', 'B7'],
#                                  'C': ['C4', 'C5', 'C6', 'C7'],
#                                  'D': ['D4', 'D5', 'D6', 'D7']},
#                                 index=[4, 5, 6, 7])

#         # setup a Mock of the extract and transform function dependencies
#         tf_func_mock = MagicMock(spec=data_to_df_func)
#         extract_func_mock = MagicMock(spec=extract_func)

#         # define the mock function behaviors when called
#         extract_func_mock.side_effect = lambda data: "extracted data"
#         tf_func_mock.side_effect = [data_df1, data_df2]
#         file_reader_func.side_effect = lambda data: "read-in json file"

#         # Act
#         result = tf_func(json_files,
#                          extract_func_mock,
#                          tf_func_mock,
#                          file_reader_func)

#         # Assert
#         # an empty dataframe is expected since three empty dataframes were
#         # merged together from the three empty json files.
#         expected_dataframe = pd.concat([data_df1, data_df2])
#         assert expected_dataframe.equals(result)

#     def test_helper_execute_json_transformation_for_one_json_succeeds(self):
#         """transforming a set of jsons in a valid directory succeeds"""

#         # Arrange
#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         transfm_fnc = c.TransformOperations.helper_execute_json_transformation
#         js_csv_fnc = c.TransformOperations.transform_news_headlines_json_to_csv
#         js_df_fnc = c.TransformOperations.transform_jsons_to_dataframe_merger
#         h_csv_fnc = c.TransformOperations.transform_headlines_dataframe_to_csv

#         # Mock out the functions that the function under test uses
#         json_csv_func = MagicMock(spec=js_csv_fnc)
#         jsons_df_func = MagicMock(spec=js_df_fnc)
#         df_csv_func = MagicMock(spec=h_csv_fnc)

#         # Mock out the behavior of the function under test, returns True
#         # indicating the single json file passed in was successfully
#         # converted to a csv
#         json_csv_func.side_effect = lambda files, filename: True

#         # setup pipeline information
#         pipeline_name = "tempus_challenge_dag"

#         headline_dir = os.path.join('tempdata',
#                                     pipeline_name,
#                                     'headlines')

#         # create dummy json file
#         full_file_path = os.path.join(headline_dir, 'dummy.json')

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory containing one json file
#             # to test the method
#             patcher.fs.create_dir(headline_dir)
#             patcher.fs.create_file(full_file_path)

#         # Act
#             # function should raise errors on an empty directory
#             result = transfm_fnc(directory=headline_dir,
#                                  json_to_csv_func=json_csv_func,
#                                  jsons_to_df_func=jsons_df_func,
#                                  df_to_csv_func=df_csv_func)

#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         # the transformation of the one json file succeeds and status returned
#         # is True
#         assert result is True

#     def test_helper_execute_json_transformation_for_three_jsons_succeeds(self):
#         """transforming a set of jsons in a valid directory succeeds"""

#         # Arrange
#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         transfm_fnc = c.TransformOperations.helper_execute_json_transformation
#         js_csv_fnc = c.TransformOperations.transform_news_headlines_json_to_csv
#         js_df_fnc = c.TransformOperations.transform_jsons_to_dataframe_merger
#         h_csv_fnc = c.TransformOperations.transform_headlines_dataframe_to_csv

#         # Mock out the functions that the function under test uses
#         json_csv_func = MagicMock(spec=js_csv_fnc)
#         jsons_df_func = MagicMock(spec=js_df_fnc)
#         df_csv_func = MagicMock(spec=h_csv_fnc)

#         # Mock out the behavior of the function under test, returns True
#         # indicating the single json file passed in was successfully
#         # converted to a csv
#         json_csv_func.side_effect = lambda files, filename: True
#         jsons_df_func.side_effect = lambda files: pd.DataFrame()
#         df_csv_func.side_effect = lambda dataframe, filename: True

#         # setup pipeline information
#         pipeline_name = "tempus_challenge_dag"

#         headline_dir = os.path.join('tempdata',
#                                     pipeline_name,
#                                     'headlines')

#         # create three dummy json files
#         full_file_path_one = os.path.join(headline_dir, 'dummy1.json')
#         full_file_path_two = os.path.join(headline_dir, 'dummy2.json')
#         full_file_path_three = os.path.join(headline_dir, 'dummy3.json')

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory containing three json file
#             # to test the method
#             patcher.fs.create_dir(headline_dir)
#             patcher.fs.create_file(full_file_path_one)
#             patcher.fs.create_file(full_file_path_two)
#             patcher.fs.create_file(full_file_path_three)

#         # Act
#             # function should raise errors on an empty directory
#             result = transfm_fnc(directory=headline_dir,
#                                  json_to_csv_func=json_csv_func,
#                                  jsons_to_df_func=jsons_df_func,
#                                  df_to_csv_func=df_csv_func)

#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         # the transformation of the one json file succeeds and status returned
#         # is True
#         assert result is True

#     def test_transform_data_to_dataframe_succeeds(self):
#         """conversion of a dictionary of numpy array news data into
#         a Pandas Dataframe succeed"""

#         # Arrange

#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         tf_func = c.TransformOperations.transform_data_to_dataframe

#         # craft the dummy extracted news data
#         extracted_data = {'source_id': ["wired"],
#                           'source_name': ["Wired"],
#                           'author': ["Klint Finley"],
#                           'title': ["Microsoft Calls a Truce in Patent Wars"],
#                           'description': ["The software giant, whose\
#                           former CEO once called\
#                           Linux a \\\"cancer,\\\" will let others use 60,000\
#                           patents for Linux-related open source projects."],
#                           'url': ["https://www.wired.com/story/microsoft-ca\
#                           lls-truce-in-linux-patent-wars/"],
#                           'url_to_image': ["https://media.wired.com/photos/5bb\
#                           e9c0f2b915f2dff96d6f4/191:100/pass/Satya-Microsoft-M\
#                           ichelleG.jpg"],
#                           'published_at': ["2018-10-11T23:33:03Z"],
#                           'content': ["Microsoft is calling for a truce in the\
#                            patent war. This week the company said it will\
#                             allow more than 2,600 other companies, including\
#                              traditional rivals like Google and IBM, to use\
#                               the technology behind 60,000 Microsoft patents\
#                                for their own Linux related o… [+4435 chars]"]}

#         # Act
#         result_df = tf_func(extracted_data)

#         # Assert
#         # because the dataframe has multiple columns which might be all tedious
#         # verifying in a single test case (according to some testing
#         # best-practices, ideally one test case should have just one assert
#         # statement), we verify the value of just three columns: source_id,
#         # source_name, and title.
#         # The three asserts are coherent in this particular instance, since
#         # they evaluate parts of the same returned object.
#         actual_source_id = result_df['news_source_id'][0]
#         actual_source_name = result_df['news_source_name'][0]
#         actual_title = result_df['news_title'][0]

#         # verify the actual result with expectations
#         assert "wired" in actual_source_id
#         assert "Wired" in actual_source_name
#         assert "Microsoft Calls a Truce in Patent Wars" in actual_title

#     def test_transform_headlines_to_csv_pipelineone_success(self,
#                                                             airflow_context,
#                                                             headline_dir_res):
#         """call to flatten jsons in the 'tempus_challenge_dag' headline
#         folder succeeds."""

#         # Arrange

#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         # get the current pipeline info
#         tf_json_func = c.TransformOperations.helper_execute_json_transformation
#         j_fn = c.TransformOperations.helper_execute_keyword_json_transformation
#         transfm_fnc = c.TransformOperations.transform_headlines_to_csv

#         # setup a Mock of the transform function dependencies
#         tf_json_func_mock = MagicMock(spec=tf_json_func)
#         tf_keyword_func_mock = MagicMock(spec=j_fn)
#         pipeline_info_obj = MagicMock(spec=c.NewsInfoDTO)
#         news_info_obj = MagicMock(spec=c.NewsInfoDTO)

#         # setup the behaviors of these Mocks
#         tf_json_func_mock.side_effect = lambda dir, exec_date: True
#         tf_keyword_func_mock.side_effect = lambda dir, exec_date: None
#         pipeline_info_obj.side_effect = lambda pipeline_name: news_info_obj
#         news_info_obj.get_headlines_directory = headline_dir_res

#         # create three dummy json files
#         full_file_path_one = os.path.join(headline_dir_res, 'dummy1.json')
#         full_file_path_two = os.path.join(headline_dir_res, 'dummy2.json')
#         full_file_path_three = os.path.join(headline_dir_res, 'dummy3.json')

#         # setup a fake headlines directory which the function under test
#         # requires be already existent
#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and files to test the method
#             patcher.fs.create_dir(headline_dir_res)
#             patcher.fs.create_file(full_file_path_one)
#             patcher.fs.create_file(full_file_path_two)
#             patcher.fs.create_file(full_file_path_three)

#         # Act
#             result = transfm_fnc(pipeline_information=pipeline_info_obj,
#                                  tf_json_func=tf_json_func_mock,
#                                  tf_key_json_func=tf_keyword_func_mock,
#                                  **airflow_context)

#         # Assert
#         # return status of the transformation operation should be True to
#         # indicate success
#         assert result is True

#     def test_transform_headlines_to_csv_pipelinetwo_success(self,
#                                                             headline_dir_res,
#                                                             airflow_context):
#         """call to flatten jsons in the 'tempus_bonus_challenge_dag' headline
#         folder succeeds."""

#         # Arrange

#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         # get the current pipeline info
#         tf_json_func = c.TransformOperations.helper_execute_json_transformation
#         j_fn = c.TransformOperations.helper_execute_keyword_json_transformation
#         transfm_fnc = c.TransformOperations.transform_headlines_to_csv

#         # setup a Mock of the transform function dependencies
#         tf_json_func_mock = MagicMock(spec=tf_json_func)
#         tf_keyword_func_mock = MagicMock(spec=j_fn)
#         pipeline_info_obj = MagicMock(spec=c.NewsInfoDTO)
#         news_info_obj = MagicMock(spec=c.NewsInfoDTO)

#         # setup the behaviors of these Mocks
#         tf_json_func_mock.side_effect = lambda dir, exec_date: None
#         tf_keyword_func_mock.side_effect = lambda dir, exec_date: True
#         pipeline_info_obj.side_effect = lambda pipeline_name: news_info_obj

#         # setup the information about the active pipeline
#         pipeline_name = 'tempus_bonus_challenge_dag'
#         airflow_context['dag'].dag_id = pipeline_name
#         headlines_directory = os.path.join('tempdata',
#                                            pipeline_name,
#                                            'headlines')

#         news_info_obj.get_headlines_directory = headlines_directory

#         # create three dummy json files
#         full_file_path_one = os.path.join(headlines_directory, 'dummy1.json')
#         full_file_path_two = os.path.join(headlines_directory, 'dummy2.json')
#         full_file_path_three = os.path.join(headlines_directory, 'dummy3.json')

#         # setup a fake headlines directory which the function under test
#         # requires be already existent
#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and files to test the method
#             patcher.fs.create_dir(headlines_directory)
#             patcher.fs.create_file(full_file_path_one)
#             patcher.fs.create_file(full_file_path_two)
#             patcher.fs.create_file(full_file_path_three)

#         # Act
#             result = transfm_fnc(pipeline_information=pipeline_info_obj,
#                                  tf_json_func=tf_json_func_mock,
#                                  tf_key_json_func=tf_keyword_func_mock,
#                                  **airflow_context)

#         # Assert
#         # return status of the transformation operation should be True to
#         # indicate success
#         assert result is True

#     @patch('pandas.read_json', autospec=True)
#     def test_transform_news_headlines_one_json_to_csv_succeeds(self,
#                                                                reader_func,
#                                                                csv_dir_res):
#         """transform of a single news headline json file to csv succeeds."""

#         # Arrange

#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         # get the current pipeline info
#         transform_func = c.TransformOperations.transform_data_to_dataframe
#         extract_func = c.ExtractOperations.extract_news_data_from_dataframe
#         trnsfm_fnc = c.TransformOperations.transform_news_headlines_json_to_csv

#         # setup a Mock of the transform function dependencies
#         transform_func_func_mock = MagicMock(spec=transform_func)
#         extract_func_mock = MagicMock(spec=extract_func)
#         transformed_data_df = MagicMock(spec=pandas.DataFrame)

#         # setup the behaviors of these Mocks
#         extract_func_mock.side_effect = lambda data: "extracted data"
#         transform_func_func_mock.side_effect = lambda data: transformed_data_df
#         reader_func.side_effect = lambda json_file: "success reading json"

#         # create one dummy json file
#         full_file_path = os.path.join(csv_dir_res, 'dummy.json')

#         # setup a fake headlines directory which the function under test
#         # requires be already existent
#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and files to test the method
#             patcher.fs.create_dir(csv_dir_res)
#             patcher.fs.create_file(full_file_path)

#             # calling the transformed DataFrame's to_csv() creates a new
#             # csv file in the fake directory
#             transformed_data_df.to_csv.side_effect = patcher.fs.create_file

#         # Act
#             result = trnsfm_fnc(full_file_path,
#                                 "dummy.csv",
#                                 extract_func=extract_func_mock,
#                                 transform_func=transform_func_func_mock,
#                                 read_js_func=reader_func)

#         # Assert
#         # return status of the transformation operation should be True to
#         # indicate success
#         assert result is True

#     @patch('pandas.read_json', autospec=True)
#     def test_transform_news_headlines_one_json_to_csv_fails(self,
#                                                             reader_func,
#                                                             csv_dir_res):
#         """transform of a single news headline json file to csv fails."""

#         # Arrange

#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         # get the current pipeline info
#         transform_func = c.TransformOperations.transform_data_to_dataframe
#         extract_func = c.ExtractOperations.extract_news_data_from_dataframe
#         trnsfm_fnc = c.TransformOperations.transform_news_headlines_json_to_csv

#         # setup a Mock of the transform function dependencies
#         transform_func_func_mock = MagicMock(spec=transform_func)
#         extract_func_mock = MagicMock(spec=extract_func)
#         transformed_data_df = MagicMock(spec=pandas.DataFrame)

#         # setup the behaviors of these Mocks
#         extract_func_mock.side_effect = lambda data: "extracted data"
#         transform_func_func_mock.side_effect = lambda data: transformed_data_df
#         reader_func.side_effect = lambda json_file: "success reading json"

#         # create one dummy json file
#         full_file_path = os.path.join(csv_dir_res, 'dummy.json')

#         # setup a fake headlines directory which the function under test
#         # requires be already existent
#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and files to test the method
#             patcher.fs.create_dir(csv_dir_res)

#             # calling the transformed DataFrame's to_csv() fails and
#             # doesn't created a new file in the fake directory
#             transformed_data_df.to_csv.side_effect = lambda *args: None

#         # Act
#             result = trnsfm_fnc(full_file_path,
#                                 "dummy.csv",
#                                 extract_func=extract_func_mock,
#                                 transform_func=transform_func_func_mock,
#                                 read_js_func=reader_func)

#         # Assert
#         # return status of the transformation operation should be False to
#         # indicate failure - the csv file could not be created as no csv
#         # directory exists
#         assert result is False

#     @patch('pandas.read_json', autospec=True)
#     def test_transform_key_headlines_to_csv_convert_fails(self,
#                                                           file_reader_func,
#                                                           home_directory_res):
#         """call to flatten jsons in the tempus_bonus_challenge_dag headline
#         folder fails."""

#         # Arrange
#         # name of the pipeline under test
#         pipeline_name = "tempus_bonus_challenge_dag"

#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         # get the current pipeline info
#         tf_func = c.TransformOperations.transform_key_headlines_to_csv
#         extract_func = c.ExtractOperations.extract_news_data_from_dataframe

#         # create the dummy input data that will be passed to the function
#         # under test
#         dummy_json_file = None
#         transform_data_df = MagicMock(spec=pandas.DataFrame)

#         # setup a Mock of the extract and transform function dependencies
#         tf_func_mock = MagicMock(spec=tf_func)
#         extract_func_mock = MagicMock(spec=extract_func)
#         extract_func_mock.side_effect = lambda data: "extracted data"
#         tf_func_mock.side_effect = lambda data: transform_data_df
#         file_reader_func.side_effect = lambda data: "read-in json file"

#         # path to the fake csv directory the function under test
#         # uses
#         csv_dir = os.path.join(home_directory_res,
#                                'tempdata',
#                                pipeline_name,
#                                'csv')

#         # name and path to the file that will be created after transformation
#         filename = str(datetime.datetime.now()) + "_" + "sample.csv"
#         # fp = os.path.join(csv_dir, filename)

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and place the dummy csv files
#             # in that directory to test the method
#             patcher.fs.create_dir(csv_dir)

#             # calling the transformed DataFrame's to_csv() creates a new
#             # csv file in the fake directory
#             transform_data_df.to_csv.side_effect = lambda filepath: "no file"

#         # Act
#             result = tf_func(dummy_json_file,
#                              filename,
#                              extract_func_mock,
#                              tf_func_mock,
#                              file_reader_func)

#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         # return status of the operation should be False to indicate the
#         # csv file was not saved to the directory
#         assert result is False

#     def test_transform_headlines_to_csv_wrong_pipeline_fails(self,
#                                                              airflow_context,
#                                                              headline_dir_res):
#         """flattening of a set of json files to csv fails when a
#         non-existent DAG pipeline name is used.
#         """

#         # Arrange

#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         tf_json_func = c.TransformOperations.helper_execute_json_transformation
#         j_fn = c.TransformOperations.helper_execute_keyword_json_transformation
#         transfm_fnc = c.TransformOperations.transform_headlines_to_csv

#         info_obj = c.NewsInfoDTO

#         # setup a Mock of the transform function dependencies
#         tf_json_func_mock = MagicMock(spec=tf_json_func)
#         tf_keyword_json_func_mock = MagicMock(spec=j_fn)

#         # setup the dummy NewsInfoDTO class that, on intialization
#         # acquires information about a pipeline-name passed in.
#         # when it is first called, with a pipeline name, it initializes
#         # various properties and returns an instance.
#         pipeline_info_obj = MagicMock(spec=info_obj)
#         news_info_obj = MagicMock(spec=info_obj)

#         # setup the behaviors of these Mocks
#         tf_json_func_mock.side_effect = lambda dir, exec_date: None
#         tf_keyword_json_func_mock.side_effect = lambda dir, exec_date: None
#         pipeline_info_obj.side_effect = lambda pipeline_name: news_info_obj

#         # use the pytest resource representing an airflow context object
#         airflow_context['dag'].dag_id = "non_existent_pipeline_name"

#         # setup a dummy class as a Mock object initializing the property
#         # that transform_headlines_to_csv() exercises
#         news_info_obj.get_headlines_directory = "/dummy/dir/headlines"

#         # Act
#         # on calling the function with a wrong pipeline name it fails,
#         # returning False to signal a failed status
#         result = transfm_fnc(pipeline_information=pipeline_info_obj,
#                              transform_json_fnc=tf_json_func_mock,
#                              transform_key_json_fnc=tf_keyword_json_func_mock,
#                              **airflow_context)

#         # Assert
#         assert result is False

#     def test_helper_execute_json_transformation_empty_dir_fails(self):
#         """transforming a set of jsons in an empty directory fails."""

#         # Arrange
#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         transfm_fnc = c.TransformOperations.helper_execute_json_transformation
#         js_csv_fnc = c.TransformOperations.transform_news_headlines_json_to_csv
#         js_df_fnc = c.TransformOperations.transform_jsons_to_dataframe_merger
#         h_csv_fnc = c.TransformOperations.transform_headlines_dataframe_to_csv

#         # Mock out the functions that the function under test uses
#         json_csv_func = MagicMock(spec=js_csv_fnc)
#         jsons_df_func = MagicMock(spec=js_df_fnc)
#         df_csv_func = MagicMock(spec=h_csv_fnc)

#         pipeline_name = "tempus_challenge_dag"

#         headline_dir = os.path.join('tempdata',
#                                     pipeline_name,
#                                     'headlines')

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem empty directory to test the method
#             patcher.fs.create_dir(headline_dir)

#         # Act
#             # function should raise errors on an empty directory
#             with pytest.raises(FileNotFoundError) as err:
#                 transfm_fnc(directory=headline_dir,
#                             json_to_csv_func=json_csv_func,
#                             jsons_to_df_func=jsons_df_func,
#                             df_to_csv_func=df_csv_func)

#             actual_message = str(err.value)
#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert "Directory is empty" in actual_message

#     def test_helper_execute_json_transformation_no_jsons_in_dir_fails(self):
#         """transforming a set of jsons in an non-empty directory but having no
#         json files fails.
#         """

#         # Arrange
#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         transfm_fnc = c.TransformOperations.helper_execute_json_transformation
#         js_csv_fnc = c.TransformOperations.transform_news_headlines_json_to_csv
#         js_df_fnc = c.TransformOperations.transform_jsons_to_dataframe_merger
#         h_csv_fnc = c.TransformOperations.transform_headlines_dataframe_to_csv

#         # Mock out the functions that the function under test uses
#         json_csv_func = MagicMock(spec=js_csv_fnc)
#         jsons_df_func = MagicMock(spec=js_df_fnc)
#         df_csv_func = MagicMock(spec=h_csv_fnc)

#         pipeline_name = "tempus_challenge_dag"

#         headline_dir = os.path.join('tempdata',
#                                     pipeline_name,
#                                     'headlines')

#         # create dummy non-json files
#         full_file_path_one = os.path.join(headline_dir, 'stuff1.txt')
#         full_file_path_two = os.path.join(headline_dir, 'stuff2.rtf')
#         full_file_path_three = os.path.join(headline_dir, 'stuff3.doc')

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and files to test the method
#             # patcher.fs.create_dir(headline_dir)
#             patcher.fs.create_file(full_file_path_one, contents='dummy txt')
#             patcher.fs.create_file(full_file_path_two, contents='dummy rtf')
#             patcher.fs.create_file(full_file_path_three, contents='dummy doc')

#         # Act
#             # function should raise errors on an empty directory
#             with pytest.raises(FileNotFoundError) as err:
#                 transfm_fnc(directory=headline_dir,
#                             json_to_csv_func=json_csv_func,
#                             jsons_to_df_func=jsons_df_func,
#                             df_to_csv_func=df_csv_func)

#             actual_message = str(err.value)
#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert "Directory has no json-headline files" in actual_message

#     def test_transform_data_to_dataframe_fails(self):
#         """conversion of a dictionary of news data into
#         a Pandas Dataframe fails"""

#         # Arrange

#         # Function Aliases
#         # use an alias since the length of the real function call when used
#         # is more than PEP-8's 79 line-character limit.
#         tf_func = c.TransformOperations.transform_data_to_dataframe

#         # function fails when the incoming news data is empty
#         data = None

#         # Act
#         with pytest.raises(ValueError) as err:
#             tf_func(data)

#         # Assert
#         actual_message = str(err.value)
#         assert "news data argument cannot be empty" in actual_message


# @pytest.mark.uploadtests
# class TestUploadOperations:
#     """test the functions for task to upload csvs to Amazon S3."""

#     @pytest.fixture(scope='class')
#     def home_directory_res(self) -> str:
#         """returns a pytest resource - path to the Airflow Home directory."""
#         return str(os.environ['HOME'])

#     @pytest.fixture(scope='class')
#     def airflow_context(self) -> dict:
#         """returns an airflow context object for tempus_challenge_dag.

#         Mimics parts of the airflow context returned during execution
#         of the tempus_challenge_dag.

#         https://airflow.apache.org/code.html#default-variables
#         """

#         dag = MagicMock(spec=DAG)
#         dag.dag_id = "tempus_challenge_dag"

#         return {
#             'ds': datetime.datetime.now().isoformat().split('T')[0],
#             'dag': dag
#         }

#     @pytest.fixture(scope='class')
#     def bucket_names(self) -> list:
#         """returns the list of available Amazon S3 buckets."""

#         return ['tempus-challenge-csv-headlines',
#                 'tempus-bonus-challenge-csv-headlines']

#     def test_upload_csv_to_s3_success_returns_correctly(self,
#                                                         airflow_context,
#                                                         bucket_names,
#                                                         home_directory_res):
#         """tests call to boto library to upload a file is actually made
#         and return a correct status.
#         """

#         # Arrange
#         # get the current pipeline info
#         pipeline_name = airflow_context['dag'].dag_id

#         # setup a Mock of the boto3 resources and file upload functions
#         upload_client = MagicMock(spec=boto3.client('s3'))
#         resource_client = MagicMock(spec=boto3.resource('s3'))
#         upload_client.upload_file.side_effect = lambda fname, bname, key: None
#         resource_client.buckets.all.side_effect = lambda: bucket_names

#         # S3 bucket to upload the file to
#         bucket_name = 'tempus-challenge-csv-headlines'

#         # path to the fake news and csv directories the function under test
#         # uses
#         csv_dir = os.path.join(home_directory_res,
#                                'tempdata',
#                                pipeline_name,
#                                'csv')

#         news_dir = os.path.join(home_directory_res,
#                                 'tempdata',
#                                 pipeline_name,
#                                 'news')

#         # create dummy csv files that will be uploaded by the function
#         full_file_path = os.path.join(csv_dir, 'stuff.csv')

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and place the dummy csv files
#             # in that directory to test the method
#             patcher.fs.create_dir(csv_dir)
#             patcher.fs.create_dir(news_dir)
#             patcher.fs.create_file(full_file_path, contents='1,dummy,txt')

#         # Act
#             # attempt uploading a file to a valid s3 bucket
#             result = c.UploadOperations.upload_csv_to_s3(csv_dir,
#                                                          bucket_name,
#                                                          upload_client,
#                                                          resource_client,
#                                                          **airflow_context)

#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert

#         # ensure the boto3 upload_file() function was called with correct
#         # arguments, resulting outcome should be True from the function
#         assert result is True

#     def test_upload_csv_to_s3_fails_with_empty_csv_dir(self,
#                                                        airflow_context,
#                                                        bucket_names,
#                                                        home_directory_res):
#         """uploading fails if the directory is empty."""

#         # Arrange

#         # setup a Mock of the boto3 resources and file upload functions
#         upload_client = MagicMock(spec=boto3.client('s3'))
#         resource_client = MagicMock(spec=boto3.resource('s3'))
#         upload_client.upload_file.side_effect = lambda: None
#         resource_client.buckets.all.side_effect = lambda: bucket_names

#         # get the current pipeline info
#         pipeline_name = airflow_context['dag'].dag_id

#         # S3 bucket to upload the file to
#         bucket_name = 'tempus-challenge-csv-headlines'

#         # path to fake news and csv directories the function under test uses
#         csv_dir = os.path.join(home_directory_res,
#                                'tempdata',
#                                pipeline_name,
#                                'csv')

#         news_dir = os.path.join(home_directory_res,
#                                 'tempdata',
#                                 pipeline_name,
#                                 'news')

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem empty directory to test the method
#             patcher.fs.create_dir(csv_dir)
#             patcher.fs.create_dir(news_dir)

#         # Act
#             # function should raise errors on an empty directory
#             with pytest.raises(FileNotFoundError) as err:
#                 c.UploadOperations.upload_csv_to_s3(csv_dir,
#                                                     bucket_name,
#                                                     upload_client,
#                                                     resource_client,
#                                                     **airflow_context)

#             actual_message = str(err.value)
#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert "Directory is empty" in actual_message

#     def test_upload_csv_to_s3_non_existent_bucket_fails(self,
#                                                         airflow_context,
#                                                         bucket_names,
#                                                         home_directory_res):
#         """uploading fails if the s3 bucket location does not already exist."""

#         # Arrange

#         # setup a Mock of the boto3 resources and file upload functions
#         upload_client = MagicMock(spec=boto3.client('s3'))
#         resource_client = MagicMock(spec=boto3.resource('s3'))
#         upload_client.upload_file.side_effect = lambda: None
#         resource_client.buckets.all.side_effect = lambda: bucket_names

#         # get the current pipeline info
#         pipeline_name = airflow_context['dag'].dag_id

#         # S3 bucket to upload the file to
#         bucket_name = 'non-existent-bucket-name'

#         # path to fake news and csv directories the function under test uses
#         csv_dir = os.path.join(home_directory_res,
#                                'tempdata',
#                                pipeline_name,
#                                'csv')

#         news_dir = os.path.join(home_directory_res,
#                                 'tempdata',
#                                 pipeline_name,
#                                 'news')

#         # create dummy csv files that will be uploaded by the function
#         full_file_path_one = os.path.join(csv_dir, 'stuff1.csv')
#         full_file_path_two = os.path.join(csv_dir, 'stuff2.csv')
#         full_file_path_three = os.path.join(csv_dir, 'stuff3.csv')

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and place the dummy csv files
#             # in that directory to test the method
#             patcher.fs.create_dir(csv_dir)
#             patcher.fs.create_dir(news_dir)
#             patcher.fs.create_file(full_file_path_one, contents='1,dummy,txt')
#             patcher.fs.create_file(full_file_path_two, contents='2,dummy,rtf')
#             patcher.fs.create_file(full_file_path_three, contents='3,dumy,doc')

#         # Act
#             # function should raise errors on an empty directory
#             with pytest.raises(FileNotFoundError) as err:
#                 c.UploadOperations.upload_csv_to_s3(csv_dir,
#                                                     bucket_name,
#                                                     upload_client,
#                                                     resource_client,
#                                                     **airflow_context)

#             actual_message = str(err.value)
#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert "does not exist on the server" in actual_message

#     def test_upload_csv_to_s3_no_csvs_in_directory_fails(self,
#                                                          airflow_context,
#                                                          bucket_names,
#                                                          home_directory_res):
#         """function fails if there are no csv-headline files in the
#         directory.
#         """

#         # Arrange

#         # setup a Mock of the boto3 resources and file upload functions
#         upload_client = MagicMock(spec=boto3.client('s3'))
#         resource_client = MagicMock(spec=boto3.resource('s3'))
#         upload_client.upload_file.side_effect = lambda: None
#         resource_client.buckets.all.side_effect = lambda: bucket_names

#         # get the current pipeline info
#         pipeline_name = airflow_context['dag'].dag_id

#         # S3 bucket to upload the file to
#         bucket_name = 'tempus-challenge-csv-headlines'

#         # path to fake news and csv directories
#         csv_dir = os.path.join(home_directory_res,
#                                'tempdata',
#                                pipeline_name,
#                                'csv')

#         news_dir = os.path.join(home_directory_res,
#                                 'tempdata',
#                                 pipeline_name,
#                                 'news')

#         # create dummy non-csv files
#         full_file_path_one = os.path.join(csv_dir, 'stuff1.txt')
#         full_file_path_two = os.path.join(csv_dir, 'stuff2.rtf')
#         full_file_path_three = os.path.join(csv_dir, 'stuff3.doc')

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and files to test the method
#             patcher.fs.create_dir(csv_dir)
#             patcher.fs.create_dir(news_dir)
#             patcher.fs.create_file(full_file_path_one, contents='dummy txt')
#             patcher.fs.create_file(full_file_path_two, contents='dummy rtf')
#             patcher.fs.create_file(full_file_path_three, contents='dummy doc')

#         # Act
#             # function should raise errors on an empty directory
#             with pytest.raises(FileNotFoundError) as err:
#                 c.UploadOperations.upload_csv_to_s3(csv_dir,
#                                                     bucket_name,
#                                                     upload_client,
#                                                     resource_client,
#                                                     **airflow_context)

#             actual_message = str(err.value)
#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert "Directory has no csv-headline files" in actual_message

#     def test_upload_csv_to_s3_fails_with_no_bucket_name(self,
#                                                         airflow_context,
#                                                         bucket_names,
#                                                         home_directory_res):
#         """function fails if the bucket name is left blank"""

#         # Arrange

#         # setup a Mock of the boto3 resources and file upload functions
#         upload_client = MagicMock(spec=boto3.client('s3'))
#         resource_client = MagicMock(spec=boto3.resource('s3'))
#         upload_client.upload_file.side_effect = lambda: None
#         resource_client.buckets.all.side_effect = lambda: bucket_names

#         # get the current pipeline info
#         pipeline_name = airflow_context['dag'].dag_id

#         # S3 bucket to upload the file to
#         bucket_name = None

#         # path to fakes news and csv directories the function under test uses
#         csv_dir = os.path.join(home_directory_res,
#                                'tempdata',
#                                pipeline_name,
#                                'csv')

#         news_dir = os.path.join(home_directory_res,
#                                 'tempdata',
#                                 pipeline_name,
#                                 'news')

#         # create dummy non-csv files
#         full_file_path_one = os.path.join(csv_dir, 'stuff1.txt')
#         full_file_path_two = os.path.join(csv_dir, 'stuff2.rtf')
#         full_file_path_three = os.path.join(csv_dir, 'stuff3.doc')

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory and files to test the method
#             patcher.fs.create_dir(csv_dir)
#             patcher.fs.create_dir(news_dir)
#             patcher.fs.create_file(full_file_path_one, contents='dummy txt')
#             patcher.fs.create_file(full_file_path_two, contents='dummy rtf')
#             patcher.fs.create_file(full_file_path_three, contents='dummy doc')

#         # Act
#             # function should raise errors on an empty directory
#             with pytest.raises(ValueError) as err:
#                 c.UploadOperations.upload_csv_to_s3(csv_dir,
#                                                     bucket_name,
#                                                     upload_client,
#                                                     resource_client,
#                                                     **airflow_context)

#             actual_message = str(err.value)
#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert "Bucket name cannot be empty" in actual_message


# @pytest.mark.newsinfotests
# class TestNewsInfoDTO:
#     """test the functions in the NewsInfoDto class."""

#     @pytest.fixture(scope='class')
#     def home_directory_res(self) -> str:
#         """returns a pytest resource - path to the Airflow Home directory."""
#         return str(os.environ['HOME'])

#     def test_newsinfodto_initialization_pipeline1_succeeds(self):
#         """creation of a new instance of the class suceeds"""

#         # Arrange
#         pipeline_name = 'tempus_bonus_challenge_dag'

#         # Act
#         news_info_obj = c.NewsInfoDTO(pipeline_name)

#         # Assert
#         assert isinstance(news_info_obj, c.NewsInfoDTO)

#     def test_load_news_files(self, home_directory_res):
#         """function successfully loads news files and returns empty list
#         if the news directory has no files.
#         """

#         # Arrange
#         pipeline_name = "tempus_challenge_dag"

#         news_path = os.path.join(home_directory_res,
#                                  'tempdata',
#                                  pipeline_name,
#                                  'news')

#         # directory_function = MagicMock()
#         directory_function = news_path

#         with Patcher() as patcher:
#             # setup pyfakefs - the fake filesystem
#             patcher.setUp()

#             # create a fake filesystem directory to test the method
#             patcher.fs.create_dir(news_path)

#         # Act
#             news_obj = c.NewsInfoDTO(pipeline_name, directory_function)
#             files = news_obj.news_files
#             # clean up and remove the fake filesystem
#             patcher.tearDown()

#         # Assert
#         assert not files

#     def test_newsinfodto_wrong_pipeline_name_fails(self):
#         """creation of a new instance with a wrong pipeline name fails."""

#         # Arrange
#         pipeline_name = 'wrong_pipeline_name_dag'

#         # Act
#         with pytest.raises(ValueError) as err:
#             c.NewsInfoDTO(pipeline_name)

#         # Assert
#         actual_message = str(err.value)
#         assert "not valid pipeline" in actual_message

#     def test_newsinfodto_blank_pipeline_name_fails(self):
#         """creation of a new instance with a wrong pipeline name fails."""

#         # Arrange
#         pipeline_name = None

#         # Act
#         with pytest.raises(ValueError) as err:
#             c.NewsInfoDTO(pipeline_name)

#         # Assert
#         actual_message = str(err.value)
#         assert "Argument pipeline_name cannot be left blank" in actual_message
