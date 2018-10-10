"""Tempus challenge  - Unit Tests.

Defines unit tests for underlining functions to operators of tasks in the DAGs.


"""


import datetime
import json
import os


from unittest.mock import MagicMock
from unittest.mock import patch

from airflow.models import DAG

from dags import challenge as c

from pyfakefs.fake_filesystem_unittest import Patcher

import pytest

import requests


@pytest.mark.storagetests
class TestFileStorage:
    """Tests the creation of the tempoary datastores used during ETL tasks.

    Maybe mock and test that os.path.exists(directory_path) is False before
    the call and True afterwards.
    test if directory already exists after the call. VERY IMPORTANT!
    """

    @pytest.fixture(scope='class')
    def home_directory_res(self) -> str:
        """Returns a pytest resource - path to the Airflow Home directory."""
        return str(os.environ['HOME'])

    @pytest.fixture(scope='class')
    def data_directories_res(self) -> list:
        """Returns a pytest resouce - list of the names of the data directories.
        """
        return ['news', 'headlines', 'csv']

    @pytest.fixture(scope='class')
    def airflow_context(self) -> dict:
        """Returns an airflow context object for tempus_challenge_dag.

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
        """Returns an airflow context object for tempus_bonus_challange_dag.

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
        """return correct news path when called correctly with pipeline name"""

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
        """return correct headlines path when called correctly with DAG name"""

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
        """return correct csv path when called correctly with pipeline name"""

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
        """write of a invalid json data to a file (already existent
        directory fails correctly.
        """

        # Arrange
        # invalid or bad json data
        json_data = '{}{}'

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
                c.FileStorage.write_json_to_file(json_data,
                                                 datastore_folder_path,
                                                 filename="test")

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
        assert "Error Decoding - Data is not valid JSON" in actual_error

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

    @pytest.mark.skip(reason="not decided best way to get at the OSError yet")
    @patch('os.makedirs', autospec=True)
    @patch('os.path.join', autospec=True)
    def test_create_data_store_pipe2_failure(self,
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

    def test_get_news_directory_fails_with_wrong_name(self):
        """return error when function is called with wrong pipeline name"""

        # Arrange
        # Act
        # Assert
        with pytest.raises(ValueError) as err:
            c.FileStorage.get_news_directory("wrong_name_dag")
        assert "No directory path for given pipeline name" in str(err.value)

    def test_get_headlines_directory_fails_with_wrong_name(self):
        """return error when function is called with wrong pipeline name"""

        # Arrange
        # Act
        # Assert
        with pytest.raises(ValueError) as err:
            c.FileStorage.get_headlines_directory("wrong_name_dag")
        assert "No directory path for given pipeline name" in str(err.value)

    def test_get_csv_directory_fails_with_wrong_name(self):
        """return error when function is called with wrong pipeline name"""

        # Arrange
        # Act
        # Assert
        with pytest.raises(ValueError) as err:
            c.FileStorage.get_csv_directory("wrong_name_dag")
        assert "No directory path for given pipeline name" in str(err.value)


@pytest.mark.networktests
class TestNetworkOperations:
    """Tests the functions for task to get news by remote call to News APIs.
    test call is made, test call returns with valid code, test call failure,
    test error handling e.g. url is number not string, test return json goes
    into directory
    integration test for actually return json.
    """

    @patch('requests.Response', autospec=True)
    def test_get_news_http_call_success(self, response_obj):
        """returned response object has a valid 200 OK response-status code."""

        # Arrange
        # response object returns an OK status code
        response_obj.status_code = requests.codes.ok
        # configure call to the Response object's json() to return dummy data
        response_obj.json.side_effect = lambda: '{"key": "value"}'
        # retrieve the path to the folder the json file is saved to
        path = c.FileStorage.get_news_directory("tempus_challenge_dag")

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory to test the method
            patcher.fs.create_dir(path)

        # Act
            result = c.NetworkOperations.get_news(response_obj, news_dir=path)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert result[0] is True

    @patch('requests.Response', autospec=True)
    def test_get_news_http_call_failure(self, response_obj):
        """returned response object fails with failure response-status code."""

        # Arrange
        response_obj.status_code = 404

        # Act
        result = c.NetworkOperations.get_news(response_obj)

        # Assert
        assert result[0] is False

    @pytest.mark.skip
    @patch('requests.Response', autospec=True)
    def test_get_keyword_news(self, response_obj):
        """Tests the http call to retrieve news based on keywords."""
        pass

    @pytest.mark.skip
    @patch('requests.Response', autospec=True)
    def test_get_news_failure(self, response_obj):
        """Tests the failure mechanism on the news retrieval function."""
        # should raise an exception
        pass

    @pytest.mark.skip
    @patch('requests.Response', autospec=True)
    def test_get_first_keyword_news(self, response_obj):
        """Tests the retrieval of news using the 'Tempus Labs' keyword"""
        pass

    @pytest.mark.skip
    @patch('requests.Response', autospec=True)
    def test_get_second_keyword_news(self, response_obj):
        """Tests the retrieval of news using the 'Eric Lefkofsky' keyword"""
        pass

    @pytest.mark.skip
    @patch('requests.Response', autospec=True)
    def test_get_third_keyword_news(self, response_obj):
        """Tests the retrieval of news using the 'Cancer' keyword"""
        pass

    @pytest.mark.skip
    @patch('requests.Response', autospec=True)
    def test_get_fourth_keyword_news(self, response_obj):
        """Tests the retrieval of news using the 'Immunotheraphy' keyword"""
        pass


@pytest.mark.extractiontests
class TestExtractOperations:
    """Tests the functions for the task to extract headlines from data."""

    @pytest.mark.skip
    def test_retrieve_headlines(self):
        """Tests the retrieval of the top headlines"""
        pass


@pytest.mark.transformtests
class TestTransformOperations:
    """Tests the functions for task to transform json headlines to csv."""

    @pytest.mark.skip
    def test_flatten_to_csv(self):
        """Tests the function to flatten a json to csv"""
        pass


@pytest.mark.uploadtests
class TestUploadOperations:
    """Tests the functions for task to upload csvs to Amazon S3."""

    @pytest.mark.skip
    def test_upload_csv_to_s3(self):
        """Tests the uploading of csvs to an s3 location"""
        pass

    # Loop of extract-transform-load. test_process_data
    @pytest.mark.skip
    def test_source_headlines(self):
        """Tests the flattening of csvs and their s3 upload for each source"""
        pass
