
import logging
import os
import pandas as pd

from dags import challenge as c


log = logging.getLogger(__name__)

# store the current directory of the airflow home folder
# airflow creates a home environment variable pointing to the location
HOME_DIRECTORY = str(os.environ['HOME'])

# final DataFrame that will be result from the entire merge of
# transformed json new files
merged_df = pd.DataFrame()


class NewsInfoDTO:
        """information about the news data this pipeline uses.


        This class functions as a Data Transfer Object(DTO).

        The get_news_headlines() function was refactored after discovering
        that alot of its functionalities were shared with that of the function
        get_news_keyword_headlines(). In order to create a common interface to
        write to at the end of the Extraction phase (the 'E' in ETL) the author
        decided to create a separate class which abstracted much of the shared
        functionality.
        Refactoring the get_news_headlines() function also made it easier to
        unit test as well.

        # Arguments:
            :param json_data: the json news data from which the news-headlines
                will be extracted from.
            :type json_data: dict
            :param pipeline_name: name of the current DAG pipeline.
            :type pipeline_name: str
            :param dir_check_func: function that returns news directory of
                the pipeline
            :type dir_check_func: function

        # Raises:
            ValueError: if the required 'json_data' argument is left blank
            ValueError: if the required 'pipeline_name' argument is left blank
        """

        def __init__(self, pipeline_name, dir_check_func=None):
            valid_dags = ['tempus_challenge_dag', 'tempus_bonus_challenge_dag']

            if not pipeline_name:
                raise ValueError("Argument pipeline_name cannot be left blank")

            if pipeline_name not in valid_dags:
                raise ValueError("{} not valid pipeline".format(pipeline_name))

            self.pipeline = str(pipeline_name)

            # for the 'tempus_challenge_dag' pipeline we need to retrieve the
            # collated news sources json files from the upstream task
            self.news_json_files = []
            if self.pipeline == "tempus_challenge_dag":
                self.news_json_files = self.load_news_files(dir_check_func)

        @property
        def headlines_directory(self) -> str:
            """returns the path to this pipeline's headline directory."""
            return c.FileStorage.get_headlines_directory(self.pipeline)

        @property
        def news_directory(self) -> str:
            """returns the path to this pipeline's news directory."""
            return c.FileStorage.get_news_directory(self.pipeline)

        @property
        def csv_directory(self) -> str:
            """returns the path to this pipeline's csv directory."""
            return c.FileStorage.get_csv_directory(self.pipeline)

        @property
        def news_files(self) -> list:
            """returns json files in the news directory of this pipeline."""
            return self.news_json_files

        @property
        def s3_bucket_name(self) -> str:
            """returns the name of the s3 bucket that stores the csv headline
            files of this pipeline.
            """

            if self.pipeline == 'tempus_challenge_dag':
                return 'tempus-challenge-csv-headlines'
            elif self.pipeline == 'tempus_bonus_challenge_dag':
                return 'tempus-bonus-challenge-csv-headlines'
            else:
                raise ValueError("No S3 Bucket exists for this Pipeline")

        def load_news_files(self, dir_check_func=None):
            """get the contents of the pipeline's news directory."""

            files = []
            if not dir_check_func:
                dir_check_func = self.news_directory

            if dir_check_func and os.listdir(dir_check_func):
                for data_file in os.listdir(dir_check_func):
                    if data_file.endswith('.json'):
                        files.append(data_file)
            return files
