"""Tempus challenge  - Unit Tests.

Describes unit tests for the PythonOperator tasks in the DAG.
"""

import datetime
import pytest
from dags import challenge as c


class TestOperations:
    """Tests the Airflow operator functions"""

    def test_retrieve_news(self):
        """Tests the retrieval of all english news sources.

        Uses a mock of a web service call mimicking the News API.
        """
        # Arrange
        news = c.Operations()
        # Act
        result = news.retrieve_english_news()
        # Assert
        assert result == "all news"

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
