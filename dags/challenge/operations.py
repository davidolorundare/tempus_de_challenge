"""Tempus challenge  - Operations and Functions.

Describes the code definition of the PythonOperator tasks in the DAG.
The 'Tempus Bonus Challenge' dag performs similar tasks to those of the
'Tempus Challenge' dag. Hence, to encourage function reusability, all the
functions executed by both dag pipelines are implemented in the same Operations
class.
"""


import json


from airflow.models import Variable


class FileStorage:
    """Handles functionality for data storage"""

    def create_data_store(self, pipeline_name):
        """Create a set of datastore folders in the local filesystem.


        Creates a 'data' folder in the AIRFLOW_HOME directory in which
        to temporaily store the JSON data retrieved from the News API
        for further processing downstream.
        Given the name of the pipeline e.g. 'tempus_challenge' or
        'tempus_bonus_challenge' creates the appropriate subdirectories
        store the intermediary data - the extracted top-headlines and
        converted csv, before the transformed data is uploaded to its
        final destination.


        # Arguments
            pipeline_name: String name of the pipeline currently active,
                and which serves as the name of the subdirectory for datastore.
        """
        pass


class Operations:
    """Handles functionality for news retrieval"""

    def retrieve_english_news(self):
        """Returns all english news sources.

        Using the News API, a http request is made to the
        News API's 'sources' endpoint, with its 'language'
        parameter set to 'en'.
        A json object is returned containing all retrieved
        English news sources.
        Note APIKey from Variables.
        - storing apikey
        - error handling
        - parsing json
        """

        return "all news"
