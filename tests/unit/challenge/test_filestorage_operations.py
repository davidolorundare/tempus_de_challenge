"""Tempus Data Engineer Challenge  - Unit Tests.

Defines unit tests for the underlining functions the
file storage task performs in the DAGs.
"""

import datetime
import json
import os
import pytest

from unittest.mock import MagicMock
from unittest.mock import patch

from airflow.models import DAG

from dags import challenge as c

from pyfakefs.fake_filesystem_unittest import Patcher


@pytest.mark.storagetests
class TestFileStorageOperations:
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

    @patch('json.load')
    def test_json_to_dataframe_reader_successfully_reads(self, reader):
        """a given json file is read successfully."""

        # Arrange
        # dummy json data the mock function will return
        data = {"status": "ok", "totalResults": 0, "articles": []}

        # Mock out the behavior of the function under test
        reader.side_effect = lambda file: data

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a path to a fake headlines folder
            headline_dir = os.path.join('tempdata',
                                        'headlines')

            # create dummy json file
            file_path = os.path.join(headline_dir, 'dummy.json')
            file_data = '{"status": "ok", "totalResults": 0, "articles": []}'

            # create a fake filesystem directory containing one json file
            # to test the method
            patcher.fs.create_dir(headline_dir)
            patcher.fs.create_file(file_path, contents=file_data)

        # Act
            # function should raise errors on an empty directory
            result = c.FileStorage.json_to_dataframe_reader(file_path,
                                                            reader)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        expected = {"status": "ok", "totalResults": 0, "articles": []}
        assert result == expected

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

    def test_write_source_headlines_to_file_no_argument_fails(self):
        """writes of news source headlines to a directory fails with missing
        parameter.
        """

        # Arrange
        key = None
        ids = ['abc-news-au', 'bbc-news']
        names = ['ABCNews', 'BBCNews']
        hd_dir = '/tempdata/data'

        # Act
        with pytest.raises(ValueError) as err:
            c.FileStorage.write_source_headlines_to_file(ids,
                                                         names,
                                                         hd_dir,
                                                         key)

        # Assert
        actual_message = str(err.value)
        assert "is blank" in actual_message

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
