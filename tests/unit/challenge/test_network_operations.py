"""Tempus Data Engineer Challenge  - Unit Tests.

Defines unit tests for the underlining functions the
task to call remote News APIs methods performs in the DAGs.
"""

import os
import pytest
import requests

from unittest.mock import MagicMock
from unittest.mock import patch
from pyfakefs.fake_filesystem_unittest import Patcher

from dags import challenge as c


@pytest.mark.networktests
class TestNetworkOperations:
    """tests news retrieval functions doing remote calls to the News APIs."""

    @patch('requests.Response', autospec=True)
    def test_get_news_http_call_success(self, response_obj):
        """returns response object has a valid 200 OK response-status code."""

        # Arrange
        # response object returns an OK status code
        response_obj.status_code = requests.codes.ok
        # configure call to the Response object's json() to return dummy data
        response_obj.json.side_effect = lambda: {"key": "value"}
        # configure Response object 'encoding' attribute
        response_obj.encoding = "utf-8"
        # retrieve the path to the folder the json file is saved to
        path = c.FileStorage.get_news_directory("tempus_challenge_dag")

        # setup a fake environment variable. real code gets it via Python's
        # os.environ() or Airflow's Variable.get() to get the name of the
        # current pipeline.
        os_environ_variable = "tempus_challenge_dag"

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory to test the method
            patcher.fs.create_dir(path)

        # Act
            result = c.NetworkOperations.get_news(response_obj,
                                                  news_dir=path,
                                                  gb_var=os_environ_variable)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert result[0] is True

    @patch('requests.PreparedRequest', autospec=True)
    @patch('requests.Response', autospec=True)
    def test_get_news_keyword_headlines_succeeds(self, response, request):
        """call to the function returns successfully"""

        # Arrange

        # create function aliases
        keyword_headline_func = c.NetworkOperations.get_news_keyword_headlines
        storage_headline_dir_func = c.FileStorage.get_headlines_directory
        storage_news_dir_func = c.FileStorage.get_news_directory

        # response object returns an OK status code
        response.status_code = requests.codes.ok

        # configure call to the Response object's json() to return dummy data
        response.json.side_effect = lambda: {"headline":
                                             "Tempus solves Cancer"}

        # configure Response object 'encoding' attribute
        response.encoding = "utf-8"

        # configure Response object's request parameters be a dummy url
        cancer_url = "https://newsapi.org/v2/top-headlines?q=cancer&apiKey=543"
        request.path_url = "/v2/top-headlines?q=cancer&apiKey=543"
        request.url = cancer_url
        response.request = request

        # retrieve the path to the folder the json file is saved to
        path_headline = storage_headline_dir_func("tempus_bonus_challenge_dag")
        path_news = storage_news_dir_func("tempus_bonus_challenge_dag")

        # filename of the keyword headline json file that will be created
        fname = "cancer_headlines"

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory to test the method
            patcher.fs.create_dir(path_headline)
            patcher.fs.create_dir(path_news)

        # Act
            result = keyword_headline_func(response,
                                           headlines_dir=path_headline,
                                           filename=fname)

            # return to the real filesystem and clear pyfakefs resources
            patcher.tearDown()

        # Assert
        assert result is True

    @patch('requests.get', autospect=True)
    def test_get_source_headlines_call_http_successfully(self, request_method):
        """a remote call to retrieve a source's top-headlines succeeds"""

        # Arrange
        # setup a dummy URL resembling the http call to get top-headlines
        base_url = "https://newsapi.org/v2"
        endpoint = "top-headlines?"
        params = "sources=abc-news"
        id_source = params.split("=")[1]
        key = os.environ["NEWS_API_KEY"]

        # craft http request
        header = "/".join([base_url, endpoint])
        http_call = "".join([header, params])
        api_key = "apiKey=" + str(key)
        http_call_with_key = "&".join([http_call, api_key])

        # Act
        c.NetworkOperations.get_source_headlines(id_source,
                                                 header,
                                                 request_method,
                                                 key)

        # Assert
        request_method.assert_called_with(http_call_with_key)

    @patch('requests.get', autospect=True)
    def test_get_source_headlines_returns_successfully(self, request_method):
        """call to retrieve a source top-headlines makes http call correctly"""

        # Arrange
        # craft the kind of expected http response when the method is called
        response_obj = MagicMock(spec=requests.Response)
        response_obj.status_code = requests.codes.ok
        request_method.side_effect = lambda url: response_obj

        # setup a dummy URL resembling the http call to get top-headlines
        base_url = "https://newsapi.org/v2"
        endpoint = "top-headlines?"
        params = "sources=abc-news"
        id_source = params.split("=")[1]
        header = "/".join([base_url, endpoint])
        key = os.environ["NEWS_API_KEY"]

        # Act
        result = c.NetworkOperations.get_source_headlines(id_source,
                                                          header,
                                                          request_method,
                                                          key)
        # Assert
        assert result.status_code == requests.codes.ok

    @patch('requests.PreparedRequest', autospec=True)
    @patch('requests.Response', autospec=True)
    def test_get_news_keyword_headlines_fails_with_bad_request(self,
                                                               response,
                                                               request):
        """call to the function fails whenw a Response in which there
        was no query in the http request is passed"""

        # Arrange

        # create function aliases
        keyword_headline_func = c.NetworkOperations.get_news_keyword_headlines
        storage_headline_dir_func = c.FileStorage.get_headlines_directory

        # response object returns an OK status code
        response.status_code = requests.codes.ok

        # configure call to the Response object's json() to return dummy data
        response.json.side_effect = lambda: {"headline":
                                             "Tempus can solve Cancer"}

        # configure Response object 'encoding' attribute
        response.encoding = "utf-8"

        # configure Response object's request parameters be a dummy url
        cancer_url = "https://newsapi.org/v2/top-headlines?apiKey=543"
        request.path_url = "/v2/top-headlines?apiKey=543"
        request.url = cancer_url
        response.request = request

        # retrieve the path to the folder the json file is saved to
        path = storage_headline_dir_func("tempus_bonus_challenge_dag")

        # filename of the keyword headline json file that will be created
        fname = "cancer_headlines"

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory to test the method
            patcher.fs.create_dir(path)

        # Act
            with pytest.raises(KeyError) as err:
                keyword_headline_func(response,
                                      headlines_dir=path,
                                      filename=fname)

            # return to the real filesystem and clear pyfakefs resources
            patcher.tearDown()

        # Assert
        actual_message = str(err.value)
        assert "Query param not found" in actual_message

    @patch('requests.get', autospec=True)
    def test_get_source_headlines_http_call_fails(self, request_method):
        """news api call to retrieve top-headlines fails"""

        # Arrange
        # craft the kind of expected http response when the method is called
        response_obj = MagicMock(spec=requests.Response)
        response_obj.status_code = requests.codes.bad
        request_method.side_effect = lambda url: response_obj

        # setup a dummy URL resembling the http call to get top-headlines
        base_url = "https://newsapi.org/v2"
        endpoint = "top-headlines?"
        params = "sources=abc-news"
        id_source = params.split("=")[1]
        header = "/".join([base_url, endpoint])
        key = os.environ["NEWS_API_KEY"]

        # Act
        result = c.NetworkOperations.get_source_headlines(id_source,
                                                          header,
                                                          request_method,
                                                          key)
        # Assert
        assert result.status_code == requests.codes.bad_request

    @patch('requests.get', autospec=True)
    def test_get_source_headlines_no_source_fails(self, request_method):
        """call to retrieve source headlines with no-source fails"""

        # Arrange
        # setup a dummy URL resembling the http call to get top-headlines
        base_url = "https://newsapi.org/v2"
        endpoint = "top-headlines?"
        id_source = None
        header = "/".join([base_url, endpoint])
        key = os.environ["NEWS_API_KEY"]

        # Act
        with pytest.raises(ValueError) as err:
            c.NetworkOperations.get_source_headlines(id_source,
                                                     header,
                                                     request_method,
                                                     key)

        # Assert
        actual_message = str(err.value)
        assert "'source_id' cannot be left blank" in actual_message

    @patch('requests.get')
    def test_get_source_headlines_no_api_key_fails(self, request_method):
        """call to retrieve source headlines with no api key fails"""

        # Arrange
        # setup a dummy URL resembling the http call to get top-headlines
        base_url = "https://newsapi.org/v2"
        endpoint = "top-headlines?"
        params = "sources=abc-news"
        id_source = params.split("=")[1]
        header = "/".join([base_url, endpoint])

        # Act
        with pytest.raises(ValueError) as err:
            c.NetworkOperations.get_source_headlines(id_source,
                                                     header,
                                                     request_method)

        # Assert
        actual_message = str(err.value)
        assert "No News API Key found" in actual_message
