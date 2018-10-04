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

    def create_data_store(self):
        """Create a set of datastore folders in the local filesystem"""
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
