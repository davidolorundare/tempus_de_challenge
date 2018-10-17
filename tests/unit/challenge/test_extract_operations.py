"""Tempus Data Engineer Challenge  - Unit Tests.

Defines unit tests for the underlining functions for
the extraction of news data task performed in the DAGs.
"""

import json
import pandas as pd

import os
import pytest
import requests

from unittest.mock import patch

from dags import challenge as c

from pyfakefs.fake_filesystem_unittest import Patcher


@pytest.mark.extractiontests
class TestExtractOperations:
    """tests the functions for the task to extract headlines from data."""

    @pytest.fixture(scope='class')
    def home_directory_res(self) -> str:
        """returns a pytest resource - path to the Airflow Home directory."""
        return str(os.environ['HOME'])

    @patch('requests.PreparedRequest', autospec=True)
    @patch('requests.Response', autospec=True)
    def test_extract_headline_keyword_success(self, response_obj, request_obj):
        """successfully parse of request url returns keyword parameter."""

        # Arrange
        # response object returns an OK status code
        response_obj.status_code = requests.codes.ok

        # configure Response object's request parameters be a dummy url
        cancer_url = "https://newsapi.org/v2/top-headlines?q=cancer&apiKey=543"
        request_obj.path_url = "/v2/top-headlines?q=cancer&apiKey=543"
        request_obj.url = cancer_url

        response_obj.request = request_obj

        # Act
        result = c.ExtractOperations.extract_headline_keyword(response_obj)

        # Assert
        # extracted keyword parameter from the url should be 'cancer'
        assert result == "cancer"

    def test_extract_news_source_id_succeeds(self):
        """extracting the 'id' parameter of sources in a json file succeeds."""

        # parse the news json 'sources' tag for all 'id' tags

        # Arrange
        # create some dummy json resembling the valid news json data
        dummy_data = {"status": "ok", "sources": [
                     {
                      "id": "abc-news",
                      "name": "ABC News",
                      "description":
                      "Your trusted source for breaking news, analysis, exclusive \
                      interviews, headlines, and videos at ABCNews.com.",
                      "url": "https://abcnews.go.com",
                      "category": "general",
                      "language": "en",
                      "country": "us"},
                     {"id": "abc-news-au",
                      "name": "ABC News (AU)",
                      "description": "Australia's most trusted source of local, \
                      national and world news. Comprehensive, independent, \
                      in-depth analysis, the latest business, sport, weather \
                      and more.",
                      "url": "http://www.abc.net.au/news",
                      "category": "general",
                      "language": "en",
                      "country": "au"}]
                      }

        # Act
        result = c.ExtractOperations.extract_news_source_id(dummy_data)

        # Assert
        expected_ids = ["abc-news", "abc-news-au"]
        assert expected_ids == result[0]

    def test_create_news_headlines_json_returns_correctly(self):
        """return valid json news data with sources and headlines ."""

        # Arrange
        # create some dummy json resembling the final source and headline data
        dummy_data_expected = {"source": {
                               "id": "abc-news",
                               "name": "ABC NEWS"},
                               "headlines": ['top-headline1', 'top-headline2']}

        source_id = "abc-news"
        source_name = "ABC NEWS"
        top_headlines = ['top-headline1', 'top-headline2']

        # Act
        result = c.ExtractOperations.create_top_headlines_json(source_id,
                                                               source_name,
                                                               top_headlines)

        # Assert
        assert result == dummy_data_expected

    def test_extract_headlines_succeeds(self):
        """return successful extraction of headlines from json data."""

        # parse the json headlines data for all 'title' tags

        # Arrange
        # create some dummy json resembling the valid news headlines json data
        dummy_data = {"status": "ok",
                      "totalResults": 20,
                      "articles":
                      [{"source": {
                        "id": "null",
                        "name": "Espn.com"},
                        "author": "null",
                        "title": "Odell Beckham Jr. walks into locker room",
                        "description": "Odell Beckham Jr. walked off field.",
                        "url": "null",
                        "urlToImage": "null",
                        "publishedAt": "2018-10-12T04:18:45Z",
                        "content": "EAST RUTHERFORD, N.J."}]}

        # Act
        result = c.ExtractOperations.extract_news_headlines(dummy_data)

        # Assert
        expected_headlines = ["Odell Beckham Jr. walks into locker room"]
        assert result == expected_headlines

    def test_extract_jsons_source_info_succeds(self, home_directory_res):
        """list of news json successfully extracts source id and names."""

        # Arrange
        dummy_dta1 = {"status": "ok", "sources": [
                     {
                      "id": "polygon",
                      "name": "Polygon",
                      "description":
                      "Your trusted source for breaking news, analysis, exclusive \
                      interviews, games and much more.",
                      "url": "https://www.polygon.com",
                      "category": "general",
                      "language": "en",
                      "country": "us"}]
                      }

        dummy_dta2 = {"status": "ok", "sources": [
                     {
                      "id": "abc-news",
                      "name": "ABC News",
                      "description":
                      "Your trusted source for breaking news, analysis, exclusive \
                      interviews, headlines, and videos at ABCNews.com.",
                      "url": "https://abcnews.go.com",
                      "category": "general",
                      "language": "en",
                      "country": "us"},
                     {"id": "abc-news-au",
                      "name": "ABC News (AU)",
                      "description": "Australia's most trusted source of local, \
                      national and world news. Comprehensive, independent, \
                      in-depth analysis, the latest business, sport, weather \
                      and more.",
                      "url": "http://www.abc.net.au/news",
                      "category": "general",
                      "language": "en",
                      "country": "au"}]
                      }

        dummy_dta3 = {"status": "ok", "sources": [
                     {
                      "id": "bbc-news",
                      "name": "BBC News",
                      "description":
                      "Tempus daily news, analysis, exclusive \
                      interviews, headlines, and videos at BBCNews.com.",
                      "url": "https://abcnews.go.com",
                      "category": "local",
                      "language": "en",
                      "country": "ng"},
                     {"id": "abc-news-au",
                      "name": "BBC News (AU)",
                      "description": "Finding things that are of local, \
                      national and world news. Comprehensive, independent, \
                      in-depth analysis, the latest business, sport, weather \
                      and more.",
                      "category": "general",
                      "language": "en",
                      "country": "au"}]
                      }

        js_one_data = json.dumps(dummy_dta1)
        js_two_data = json.dumps(dummy_dta2)
        js_thre_data = json.dumps(dummy_dta3)
        extract_func_alias = c.ExtractOperations.extract_jsons_source_info

        news_path = os.path.join(home_directory_res, 'tempdata', 'jsons')
        js_dir = news_path
        js_list = ['stuff1.json', 'stuff2.json', 'stuff3.json']

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create dummy files
            full_file_path_one = os.path.join(js_dir, 'stuff1.json')
            full_file_path_two = os.path.join(js_dir, 'stuff2.json')
            full_file_path_three = os.path.join(js_dir, 'stuff3.json')

            # create a fake filesystem directory to test the method
            patcher.fs.create_dir(js_dir)
            patcher.fs.create_file(full_file_path_one, contents=js_one_data)
            patcher.fs.create_file(full_file_path_two, contents=js_two_data)
            patcher.fs.create_file(full_file_path_three, contents=js_thre_data)

        # Act
            actual_result = extract_func_alias(js_list, js_dir)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        expected = (['bbc-news', 'abc-news-au'], ['bbc news', 'bbc news (au)'])
        assert expected == actual_result

    def test_extract_news_data_from_dataframe_succeeds(self):
        """extraction of information from news dataframe succeeds."""

        # Arrange

        # craft the invalid dataframe data
        data = pd.DataFrame()
        total_results = [1]
        status = ['ok']

        source1 = {"source": {"id": "wired", "name": "Wired"},
                   "author": "Klint Finley", "title": "Microsoft\
                   Calls a Truce in the Linux\
                    Patent Wars", "description": "The software giant, whose\
                     former CEO once called\
                    Linux a \\\"cancer,\\\" will let others use 60,000\
                     patents for Linux-related open source pro\
                     jects.", "url": "https://www.wired.com/story/mic\
                     rosoft-calls-truce-in-linux-patent-wars/", "urlToImage": "https://med\
                     ia.wired.com/photos/5bbe9c0f2b915f2dff96d6f4/191:100/pas\
                     s/Satya-Microsoft-MichelleG.jpg", "publishedAt": "20\
                     18-10-11T23:33:03Z", "content": "Microsoft is calling\
                      for a truce in the patent war. This week the company\
                       said it will allow more than 2,600 other companies,\
                        including traditional rivals like Google and IBM,\
                         to use the technology behind 60,000 Microsoft\
                          patents for their own Linux related o… [+4435 chars]"}

        articles = [source1]

        data['status'] = status
        data['totalResults'] = total_results
        data['articles'] = articles

        # Act
        result = c.ExtractOperations.extract_news_data_from_dataframe(data)

        # Assert
        # we know the function's return value is a dictionary.
        empty_dict = None
        if not result:
            empty_dict = True
        else:
            empty_dict = False
        assert empty_dict is False

    def test_extract_news_data_from_dataframe_no_articles_fails(self):
        """extraction of information from news dataframe fails
        if there are no news articles.
        """

        # Arrange

        # craft the invalid dataframe data
        data = pd.DataFrame()
        total_results = [0, 0, 0]
        status = ['ok', 'ok', 'ok']
        articles = [None, None, None]

        data['status'] = status
        data['totalResults'] = total_results
        data['articles'] = articles

        # Act
        result = c.ExtractOperations.extract_news_data_from_dataframe(data)

        # Assert
        # we know the function's return value is a dictionary.
        empty_dict = None
        if not result:
            empty_dict = True
        else:
            empty_dict = False
        assert empty_dict is True

    def test_extract_jsons_source_info_no_data_fails(self, home_directory_res):
        """list of news json fails at extracting source id and names with
        bad data.
        """

        # Arrange
        dummy_data_one = {"sources": {
                          "id": "u'asd",
                          "name": "u'erw"},
                          "headlines": []}

        dummy_data_two = {"sources": [
                          {"id": 33435},
                          {"nafme": 'ABC News2'}],
                          "name": [
                          {"ido": 3465635},
                          {"name": 'ABC News2'}],
                          "headlines": ['headlines1', 'headlines2']}

        json_file_one_data = json.dumps(dummy_data_one)
        json_file_two_data = json.dumps(dummy_data_two)
        extract_func_alias = c.ExtractOperations.extract_jsons_source_info

        news_path = os.path.join(home_directory_res, 'tempdata', 'jsons')
        json_directory = news_path
        json_list = ['stuff1.json', 'stuff2.json']

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create dummy files
            full_file_path_one = os.path.join(json_directory, 'stuff1.json')
            full_file_path_two = os.path.join(json_directory, 'stuff2.json')

            # create a fake filesystem directory files to test the method
            patcher.fs.create_dir(json_directory)
            patcher.fs.create_file(full_file_path_one,
                                   contents=json_file_one_data,
                                   encoding="utf-16")
            patcher.fs.create_file(full_file_path_two,
                                   contents=json_file_two_data,
                                   encoding="utf-16")

        # Act
            with pytest.raises(ValueError) as err:
                extract_func_alias(json_list, json_directory)

            expected = str(err.value)
            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert "Parsing Error" in expected

    def test_extract_news_source_id_no_sources_fails(self):
        """no source tag in the json data fails the extraction process."""

        # Arrange
        # create some dummy json resembling the news json data with no
        # source tag.
        dummy_data = {"status": "ok"}

        # Act
        with pytest.raises(KeyError) as err:
            c.ExtractOperations.extract_news_source_id(dummy_data)

        # Assert
        actual_message = str(err.value)

        assert "news json has no 'sources' data" in actual_message

    def test_extract_news_source_id_empty_sources_fails(self):
        """empty source tag in the json data fails the extraction process."""

        # Arrange
        # create some dummy json resembling the news json data with an empty
        # source tag.
        dummy_data = {"status": "ok", "sources": []}

        # Act
        with pytest.raises(ValueError) as err:
            c.ExtractOperations.extract_news_source_id(dummy_data)

        # Assert
        actual_message = str(err.value)

        assert "'sources' tag in json is empty" in actual_message

    def test_extract_headlines_no_news_article_fails(self):
        """extraction of headlines with no article tag fails."""

        # Arrange
        # create some dummy json resembling the news json headline data with no
        # article tag.
        dummy_data = {"status": "ok", "totalResults": 20}

        # Act
        with pytest.raises(KeyError) as err:
            c.ExtractOperations.extract_news_headlines(dummy_data)

        # Assert
        actual_message = str(err.value)

        assert "news json has no 'articles' data" in actual_message

    def test_extract_headlines_empty_news_article_returns_empty_list(self):
        """return empty list when there is no headlines."""

        # Arrange
        # create some dummy json resembling the valid news headlines json data
        dummy_data = {"status": "ok",
                      "totalResults": 20,
                      "articles": []}

        # Act
        result = c.ExtractOperations.extract_news_headlines(dummy_data)

        # Assert
        assert result == []

    def test_create_news_headlines_json_no_source_id_fails(self):
        """passing no source_id to the function raises errors."""

        # Arrange
        source_id = None
        source_name = "ABC NEWS"
        top_headlines = ['top-headline1', 'top-headline2']

        # Act
        with pytest.raises(ValueError) as err:
            c.ExtractOperations.create_top_headlines_json(source_id,
                                                          source_name,
                                                          top_headlines)

        # Assert
        actual_message = str(err.value)
        assert "'source_id' cannot be blank" in actual_message

    def test_create_news_headlines_json_no_source_name_fails(self):
        """passing no source_name to the function raises errors."""

        # Arrange
        source_id = "abc-news"
        source_name = None
        top_headlines = ['top-headline1', 'top-headline2']

        # Act
        with pytest.raises(ValueError) as err:
            c.ExtractOperations.create_top_headlines_json(source_id,
                                                          source_name,
                                                          top_headlines)

        # Assert
        actual_message = str(err.value)
        assert "'source_name' cannot be blank" in actual_message

    def test_create_news_headlines_json_no_headlines_list_fails(self):
        """passing no list of headlines to the function raises errors."""

        # Arrange
        source_id = "abc-news"
        source_name = "ABC NEWS"
        top_headlines = None

        # Act
        with pytest.raises(ValueError) as err:
            c.ExtractOperations.create_top_headlines_json(source_id,
                                                          source_name,
                                                          top_headlines)

        # Assert
        actual_message = str(err.value)
        assert "'headlines' cannot be blank" in actual_message

    @patch('requests.PreparedRequest', autospec=True)
    @patch('requests.Response', autospec=True)
    def test_extract_headline_keyword_no_query_in_url_fails(self,
                                                            response_obj,
                                                            request_obj):
        """request url with no query keyword parameter fails."""

        # Arrange
        # response object returns an OK status code
        response_obj.status_code = requests.codes.ok

        # configure Response object's request parameters be a dummy url
        cancer_url = "https://newsapi.org/v2/top-headlines?apiKey=543"
        request_obj.path_url = "/v2/top-headlines?apiKey=543"
        request_obj.url = cancer_url

        response_obj.request = request_obj

        # Act
        with pytest.raises(KeyError) as err:
            c.ExtractOperations.extract_headline_keyword(response_obj)

        # Assert
        actual_message = str(err.value)
        assert "Query param not found in URL" in actual_message
