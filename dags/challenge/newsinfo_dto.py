
import logging
import os

from dags import challenge as c


log = logging.getLogger(__name__)

# store the current directory of the airflow home folder
# airflow creates a home environment variable pointing to the location
HOME_DIRECTORY = str(os.environ['HOME'])


class NewsInfoDTO:
        """Information-object about the news data this pipeline uses.


        This class functions as a Data Transfer Object(DTO).

        The get_news_headlines() function was refactored after discovering
        that alot of its functionalities were shared with that of the function
        get_news_keyword_headlines(). In order to create a common interface to
        write to at the end of the Extraction phase (the 'E' in ETL) the author
        decided to create a separate class which abstracted much of the shared
        functionality.
        Refactoring the get_news_headlines() function also made it easier to
        unit test it better as well.

        # Arguments:
            :param pipeline_name: name of the current DAG pipeline.
            :type pipeline_name: str
            :param news_dir_path: path to the news directory of this pipeline.
            :type news_dir_path: function

        # Raises:
            ValueError: if the required 'pipeline_name' argument entered is
                not valid.
            ValueError: if the required 'pipeline_name' argument is left blank.
        """

        def __init__(self, pipeline_name, news_dir_path=None):
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
                self.news_json_files = self.load_news_files(news_dir_path)

        @property
        def headlines_directory(self) -> str:
            """Returns the path to this pipeline's headline directory."""
            return c.FileStorage.get_headlines_directory(self.pipeline)

        @property
        def news_directory(self) -> str:
            """Returns the path to this pipeline's news directory."""
            return c.FileStorage.get_news_directory(self.pipeline)

        @property
        def csv_directory(self) -> str:
            """Returns the path to this pipeline's csv directory."""
            return c.FileStorage.get_csv_directory(self.pipeline)

        @property
        def news_files(self) -> list:
            """Returns json files in the news directory of this pipeline."""
            return self.news_json_files

        @property
        def s3_bucket_name(self) -> str:
            """Returns the name of the s3 bucket that stores the csv-headline
            files of this pipeline.
            """

            if self.pipeline == 'tempus_challenge_dag':
                return 'tempus-challenge-csv-headlines'
            elif self.pipeline == 'tempus_bonus_challenge_dag':
                return 'tempus-bonus-challenge-csv-headlines'
            else:
                raise ValueError("No S3 Bucket exists for this Pipeline")

        def load_news_files(self, news_dir_path=None):
            """Gets the file contents of the pipeline's news directory."""

            files = []
            if not news_dir_path:
                news_dir_path = self.news_directory

            if news_dir_path and os.listdir(news_dir_path):
                for data_file in os.listdir(news_dir_path):
                    if data_file.endswith('.json'):
                        files.append(data_file)
            return files
