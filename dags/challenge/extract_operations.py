
import numpy as np
import json
import logging
import os
import requests
import pandas as pd


log = logging.getLogger(__name__)

# store the current directory of the airflow home folder
# airflow creates a home environment variable pointing to the location
HOME_DIRECTORY = str(os.environ['HOME'])

# final DataFrame that will be result from the entire merge of
# transformed json new files
merged_df = pd.DataFrame()


class ExtractOperations:
    """handles functionality for extracting headlines."""

    @classmethod
    def create_top_headlines_json(cls, source_id, source_name, headlines):
        """creates a json object out of given news source and its headlines.

        # Arguments:
            :param source_id: the id of the news source
            :type source_id: str
            :param source_name: the name of the news source
            :type source_name: str
            :param headlines: list containing all the headlines of the news
                source, each as a string.
            :type headlines: list

        # Raises:
            ValueError: if the no source_id is given
            ValueError: if the no source_name is given
            ValueError: if the no headlines is given
        """

        log.info("Running create_top_headlines_json method")

        if not source_id:
            raise ValueError("'source_id' cannot be blank")
        if not source_name:
            raise ValueError("'source_name' cannot be blank")
        if not headlines:
            raise ValueError("'headlines' cannot be blank")

        source_top_headlines = {"source": {
                                "id": source_id,
                                "name": source_name},
                                "headlines": headlines}

        return source_top_headlines

    @classmethod
    def extract_news_source_id(cls, json_data):
        """returns a list of (string) news source ids from a valid json.

        # Arguments:
            :param json_data: the json news data from which the news-source
                ids will be extracted from.
            :type json_data: dict

        # Raises:
            KeyError: if the given json news data does not have the 'sources'
                tag.
            ValueError: if the given json news data has a 'sources' tag with
                empty data
        """

        log.info("Running extract_news_source_id method")

        if "sources" not in json_data.keys():
            raise KeyError("news json has no 'sources' data")

        if not json_data["sources"]:
            raise ValueError("'sources' tag in json is empty")

        sources_ids = []
        sources_names = []

        for source in json_data["sources"]:
            ids = source["id"]
            sources_ids.append(str(ids).lower())
            name = source["name"]
            sources_names.append(str(name).lower())

        return sources_ids, sources_names

    @classmethod
    def extract_news_headlines(cls, json_data):
        """returns a list of (string) news headlines from a valid json.

        # Arguments:
            :param json_data: the json news data from which the news-headlines
                will be extracted from.
            :type json_data: dict

        # Raises:
            KeyError: if the given json news data has no news 'articles'
            ValueError: if the given json news headlines data has a
                'articles' tag with empty data.
        """

        log.info("Running extract_news_headlines method")

        if "articles" not in json_data.keys():
            raise KeyError("news json has no 'articles' data")

        if not json_data["articles"]:
            print("There are no headlines - 'articles' tag in json is empty")
            return []

        # Get all the articles for this news source
        news_articles = [article for article in json_data["articles"]]

        # Get the headline of each news article
        headlines = [article["title"] for article in news_articles]

        return headlines

    @classmethod
    def extract_headline_keyword(cls, response: requests.Response):
        """extract string query keyword used to request given http Response.

        It specficially stores the query keyword, from the http Response
        object, that was applied during the http request to the 'top-headlines'
        News API endpoint.
        """

        log.info("Running extract_headline_keyword method")

        # inspect the request's Response object URL
        relative_url = response.request.path_url
        full_url = response.request.url

        # parse the url
        remove_relative_base_url = relative_url.split("?")
        # base_url = remove_relative_base_url[0]
        url_params = remove_relative_base_url[1]

        # extract the parameters
        parameter_list = []
        if '&' in url_params:
            parameter_list = url_params.split("&")
        else:
            parameter_list.append(url_params)

        # extract the query parameter 'q'
        query = [key for key in parameter_list if key.startswith('q')]

        # error check
        if not query:
            raise KeyError("Query param not found in URL {}".format(full_url))

        # there should only be one keyword variable in the url
        keyword = query[0].split("=")[1]
        query_keyword = str(keyword)

        return query_keyword.lower()

    @classmethod
    def extract_jsons_source_info(cls, json_list, json_directory):
        """parses a given list of news jsons for their source ids and names.

        Returns a tuple of the source id and name.

        # Arguments:
            :param json_list: list of jsons whose source info is to be parsed
            :type json_list: list
            :param json_directory: directory where these json files are stored.
            :type json_directory: str

        # Raises:
            ValueError: if an error during parsing a json file is found
        """

        log.info("Running extract_jsons_source_info method")

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        source_extract_func = cls.extract_news_source_id

        # process the collated json files
        for js in json_list:
            json_path = os.path.join(json_directory, js)

            # read each news json and extract the news sources
            with open(json_path, "r") as js_file:
                try:
                    raw_data = json.load(js_file)
                    extracted_sources = source_extract_func(raw_data)
                except ValueError:
                    raise ValueError("Parsing Error: {}".format(json_path))

        return extracted_sources

    @classmethod
    def extract_news_data_from_dataframe(cls, frame):
        """returns extracted information from a news dataframe.

        Based on the API documentation for the top-headlines, for the
        returned response, the fields that we need from each json object
        are:

        the identification id of the news source
        - source id

        the name of the news source
        - source name

        the author of the new article
        - author

        the headline or title of the article
        - title

        description from the article
        - description

        url to the article
        - url

        a url to a relevant image for the article
        - urlToImage

        the date and time the article was published
        - publishedAt

        the unformatted content of the article
        - content

        Returns a dictionary containing all these extracted information.

        # Arguments:
            :param frame: a Pandas DataFrame containing news data
            :type frame: DataFrame
        """

        log.info("Running extract_news_data_from_dataframe method")

        num_of_articles = frame['totalResults'][0]
        # dictionary representing the extracted news data
        extracted_data = {}

        # error check - no articles means this json had no news data
        if num_of_articles < 1:
            return extracted_data

        # Using Pandas, extract required information from the given dataframe
        # each is a return list of data.
        source_id = [frame['articles'][index]['source']['id']
                     for index in np.arange(num_of_articles)]

        source_name = [frame['articles'][index]['source']['name']
                       for index in np.arange(num_of_articles)]

        author = [frame['articles'][index]['author']
                  for index in np.arange(num_of_articles)]

        title = [frame['articles'][index]['title']
                 for index in np.arange(num_of_articles)]

        description = [frame['articles'][index]['description']
                       for index in np.arange(num_of_articles)]

        url = [frame['articles'][index]['url']
               for index in np.arange(num_of_articles)]

        url_to_image = [frame['articles'][index]['urlToImage']
                        for index in np.arange(num_of_articles)]

        published_at = [frame['articles'][index]['publishedAt']
                        for index in np.arange(num_of_articles)]

        content = [frame['articles'][index]['content']
                   for index in np.arange(num_of_articles)]

        # compose a dictionary with the extracted information
        extracted_data = {'source_id': source_id,
                          'source_name': source_name,
                          'author': author,
                          'title': title,
                          'description': description,
                          'url': url,
                          'url_to_image': url_to_image,
                          'published_at': published_at,
                          'content': content}

        return extracted_data
