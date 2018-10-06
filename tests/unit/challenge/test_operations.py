"""Tempus challenge  - Unit Tests.

Defines unit tests for underlining functions to operators of tasks in the DAGs.


"""


import datetime
import os
from unittest.mock import MagicMock
from unittest.mock import patch

from airflow.models import DAG

from dags import challenge as c

import pytest


class TestFileStorage:
    """Tests the creation of the tempoary datastores used during ETL tasks."""

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
        dag.dag_id.return_value = "tempus_challenge_dag"

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
        dag.dag_id.return_value = "tempus_bonus_challenge_dag"

        return {
            'ds': datetime.datetime.now().isoformat().split('T')[0],
            'dag': dag
        }

    @pytest.mark.skip
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_create_news_data_store_pipe1_success(self,
                                                  mock_path_func,
                                                  mock_dir_func,
                                                  home_directory_res,
                                                  data_directories_res,
                                                  airflow_context):
        """Tests the call to create the tempoary 'news' datastore folders used
        by the tempus_challenge_dag operators.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_store(mock_path_func,
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

    @pytest.mark.skip
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_create_headlines_data_store_pipe1_success(self,
                                                       mock_path_func,
                                                       mock_dir_func,
                                                       home_directory_res,
                                                       data_directories_res,
                                                       airflow_context):
        """Tests the call to create the tempoary 'headlines' datastore folders used
        by the tempus_challenge_dag operators.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_store(mock_path_func,
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

    @pytest.mark.skip
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_create_csv_data_store_pipe1_success(self,
                                                 mock_path_func,
                                                 mock_dir_func,
                                                 home_directory_res,
                                                 data_directories_res,
                                                 airflow_context):
        """Tests the call to create the tempoary 'csv' datastore folders used
        by the tempus_challenge_dag operators.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_store(mock_path_func,
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

    @pytest.mark.skip
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_create_news_data_store_pipe2_success(self,
                                                  mock_path_func,
                                                  mock_dir_func,
                                                  home_directory_res,
                                                  data_directories_res,
                                                  airflow_context_bonus):
        """Tests the call to create the tempoary 'news' datastore folders used
        by the tempus_bonus_challenge_dag operators.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_store(mock_path_func,
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

    @pytest.mark.skip
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_create_headlines_data_store_pipe2_success(self,
                                                       mock_path_func,
                                                       mock_dir_func,
                                                       home_directory_res,
                                                       data_directories_res,
                                                       airflow_context_bonus):
        """Tests the call to create the tempoary 'headlines' datastore folders used
        by the tempus_bonus_challenge_dag operators.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_store(mock_path_func,
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

    @pytest.mark.skip
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_create_csv_data_store_pipe2_success(self,
                                                 mock_path_func,
                                                 mock_dir_func,
                                                 home_directory_res,
                                                 data_directories_res,
                                                 airflow_context_bonus):
        """Tests the call to create the tempoary 'csv' datastore folders used
        by the tempus_bonus_challenge_dag operators.
        """

        # Arrange

        # Act
        c.FileStorage.create_data_store(mock_path_func,
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


class TestNetworkOperations:
    """Tests the functions for task to get news by remote call to News APIs."""

    @pytest.mark.skip
    def test_retrieve_keyword_news(self):
        """Tests the http call to retrieve news based on keywords."""
        pass

    @pytest.mark.skip
    def test_retrieve_news_should_return_valid_status_code(self):
        """Tests that the http call return status code is for a valid response.

        mock that the return status code is 200
        """
        pass

    def test_retrieve_news_should_use_http_lib_properly(self):
        """Tests the http call to retrieve all english news sources.

        Uses a mock of a web service call mimicking the News API.
        Create a mock of the english new sources call which simulates
        the *request* and *response*.
        ISSUES:
        - creating service mock
        - storing apikey
        - error handling
        - parsing json
        - data store
        """
        # Arrange
        address = MagicMock()
        news = c.NetworkOperations()
        # Act
        result = news.retrieve_english_news(address)
        # Assert
        assert result == "all news"

    @pytest.mark.skip
    def test_retrieve_news_failure(self):
        """Tests the failure mechanism on the news retrieval function."""
        # should raise an exception
        pass

    @pytest.mark.skip
    def test_retrieve_first_keyword_news(self):
        """Tests the retrieval of news using the 'Tempus Labs' keyword"""
        pass

    @pytest.mark.skip
    def test_retrieve_second_keyword_news(self):
        """Tests the retrieval of news using the 'Eric Lefkofsky' keyword"""
        pass

    @pytest.mark.skip
    def test_retrieve_third_keyword_news(self):
        """Tests the retrieval of news using the 'Cancer' keyword"""
        pass

    @pytest.mark.skip
    def test_retrieve_fourth_keyword_news(self):
        """Tests the retrieval of news using the 'Immunotheraphy' keyword"""
        pass


class TestExtractOperations:
    """Tests the functions for the task to extract headlines from data."""

    @pytest.mark.skip
    def test_retrieve_headlines(self):
        """Tests the retrieval of the top headlines"""
        pass


class TestTransformOperations:
    """Tests the functions for task to transform json headlines to csv."""

    @pytest.mark.skip
    def test_flatten_to_csv(self):
        """Tests the function to flatten a json to csv"""
        pass


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
