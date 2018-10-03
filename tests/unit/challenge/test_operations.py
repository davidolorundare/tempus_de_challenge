"""Tempus challenge  - Unit Tests.

Describes unit tests for the PythonOperator tasks in the DAG.
"""

import datetime
import pytest
from dags import challenge as c


class TestOperations:
    """Tests the Airflow operator functions"""

    def test_retrieve_news(self):
        """Tests the retrieval of all english news sources"""
        # Arrange
        news = c.RetrieveEnglishNews()
        # Act
        result = news.retrieve()
        # Assert
        assert result == "all news"
