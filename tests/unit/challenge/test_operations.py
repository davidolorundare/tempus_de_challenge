"""Tempus challenge  - Unit Tests.

Describes unit tests for the PythonOperator tasks in the DAG.
"""


import datetime
import pytest
from unittest.mock import patch
from unittest.mock import MagicMock

from dags import challenge as c
from airflow.models import DAG



class TestFileStorage:
    """Tests the creation of datastores.

    Will need to split out to testnetworkoperations, testextractioperations,
    testtransformoperations, and testloadoperations, testprocess_data
    """

    @pytest.fixture(scope='class')
    def context(self) -> dict:
        """https://airflow.apache.org/code.html#default-variables"""
        dag = MagicMock(spec=DAG)
        dag.dag_id.return_value = "tempus_challenge_dag"

        return {
            'ds': datetime.datetime.now().isoformat().split('T')[0],
            'dag': dag
        }

    @pytest.fixture(scope='class')
    def context_bonus(self) -> dict:
        """https://airflow.apache.org/code.html#default-variables"""
        dag = MagicMock(spec=DAG)
        dag.dag_id.return_value = "tempus_bonus_challenge_dag"

        return {
            'ds': datetime.datetime.now().isoformat().split('T')[0],
            'dag': dag
        }

    @pytest.mark.skip
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_create_data_store_successfully_first_pipe(self,
                                                       mock_path_func,
                                                       mock_dir_func,
                                                       context):
        """Tests the creation of a tempoary data storage folder"""
        directories = c.FileStorage.create_data_store(mock_path_func,
                                                      mock_dir_func,
                                                      **context)

    @pytest.mark.skip
    @patch('os.makedirs')
    @patch('os.path.join')
    def test_create_data_store_successfully_second_pipe(self,
                                                        mock_path_func,
                                                        mock_dir,
                                                        context_bonus):
        """Tests the creation of a tempoary data storage folder"""
        directories = c.FileStorage.create_data_store(mock_path_func,
                                                      mock_dir_func,
                                                      **context_bonus)


class TestOperations:
    """Tests the Airflow operator functions.

    Will need to split out to testnetworkoperations, testextractioperations,
    testtransformoperations, and testloadoperations, testprocess_data
    """

    @pytest.mark.skip
    def test_retrieve_keyword_news(self):
        """Tests the retrieval of news based on keywords"""
        pass

    def test_retrieve_news_should_return_valid_status_code(self):
        """Tests that the return status code is for a valid response"""
        #mock that the return status code is 200
        pass

    def test_retrieve_news_should_use_http_lib_properly(self):
        """Tests the retrieval of all english news sources.

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
        news = c.Operations()
        # Act
        result = news.retrieve_english_news()
        # Assert
        assert result == "all news"

    @pytest.mark.skip
    def test_retrieve_news_failure(self):
        """Tests the failure mechanism of the news retrieval function"""
        # should raise an exception
        pass

    @pytest.mark.skip
    def test_retrieve_headlines(self):
        """Tests the retrieval of the top headlines"""
        pass

    @pytest.mark.skip
    def test_flatten_to_csv(self):
        """Tests the flattening of a json to csv"""
        pass

    @pytest.mark.skip
    def test_upload_csv_to_s3(self):
        """Tests the uploading of csvs to an s3 location"""
        pass

    @pytest.mark.skip
    def test_source_headlines(self):
        """Tests the flattening of csvs and their s3 upload for each source"""
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
