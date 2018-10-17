"""Tempus Data Engineer Challenge  - Unit Tests.

Defines unit tests for the helper DTO class 'NewsInfoDTO'
used by several functions in the project.
"""

import os
import pytest

from dags import challenge as c

from pyfakefs.fake_filesystem_unittest import Patcher


@pytest.mark.newsinfotests
class TestNewsInfoDTO:
    """test the functions in the NewsInfoDto class."""

    @pytest.fixture(scope='class')
    def home_directory_res(self) -> str:
        """returns a pytest resource - path to the Airflow Home directory."""
        return str(os.environ['HOME'])

    def test_newsinfodto_initialization_pipeline1_succeeds(self):
        """creation of a new instance of the class suceeds"""

        # Arrange
        pipeline_name = 'tempus_bonus_challenge_dag'

        # Act
        news_info_obj = c.NewsInfoDTO(pipeline_name)

        # Assert
        assert isinstance(news_info_obj, c.NewsInfoDTO)

    def test_load_news_files(self, home_directory_res):
        """function successfully loads news files and returns empty list
        if the news directory has no files.
        """

        # Arrange
        pipeline_name = "tempus_challenge_dag"

        news_path = os.path.join(home_directory_res,
                                 'tempdata',
                                 pipeline_name,
                                 'news')

        # directory_function = MagicMock()
        directory_function = news_path

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory to test the method
            patcher.fs.create_dir(news_path)

        # Act
            news_obj = c.NewsInfoDTO(pipeline_name, directory_function)
            files = news_obj.news_files
            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert not files

    def test_newsinfodto_wrong_pipeline_name_fails(self):
        """creation of a new instance with a wrong pipeline name fails."""

        # Arrange
        pipeline_name = 'wrong_pipeline_name_dag'

        # Act
        with pytest.raises(ValueError) as err:
            c.NewsInfoDTO(pipeline_name)

        # Assert
        actual_message = str(err.value)
        assert "not valid pipeline" in actual_message

    def test_newsinfodto_blank_pipeline_name_fails(self):
        """creation of a new instance with a wrong pipeline name fails."""

        # Arrange
        pipeline_name = None

        # Act
        with pytest.raises(ValueError) as err:
            c.NewsInfoDTO(pipeline_name)

        # Assert
        actual_message = str(err.value)
        assert "Argument pipeline_name cannot be left blank" in actual_message
