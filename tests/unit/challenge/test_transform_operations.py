"""Tempus Data Engineer Challenge  - Unit Tests.

Defines unit tests for the underlining functions for
the transforming the extracted of news data task performed in the DAGs.
"""

import datetime
import pandas as pd
import pandas
import os
import pytest

from unittest.mock import MagicMock
from unittest.mock import patch

from airflow.models import DAG

from dags import challenge as c

from pyfakefs.fake_filesystem_unittest import Patcher


@pytest.mark.transformtests
class TestTransformOperations:
    """test the functions for task to transform json headlines to csv."""

    @pytest.fixture(scope='class')
    def airflow_context(self) -> dict:
        """returns an airflow context object for tempus_challenge_dag.

        Mimics parts of the airflow context returned during execution
        of the tempus_challenge_dag.

        https://airflow.apache.org/code.html#default-variables
        """

        dag = MagicMock(spec=DAG)
        dag.dag_id = "tempus_challenge_dag"
        current_execution_time = str(datetime.datetime.now())

        return {
            'dag': dag,
            'execution_date': current_execution_time
        }

    @pytest.fixture(scope='class')
    def home_directory_res(self) -> str:
        """returns a pytest resource - path to the Airflow Home directory."""
        return str(os.environ['HOME'])

    @pytest.fixture(scope='class')
    def headline_dir_res(self) -> str:
        """returns a pytest resource - path to the 'tempus_challenge_dag'
        pipeline headlines directory.
        """

        headlines_path = os.path.join(self.home_directory_res(),
                                      'tempdata',
                                      'tempus_challenge_dag',
                                      'headlines')
        return headlines_path

    @pytest.fixture(scope='class')
    def csv_dir_res(self) -> str:
        """returns a pytest resource - path to the 'tempus_challenge_dag'
        pipeline csv directory.
        """

        csv_path = os.path.join(self.home_directory_res(),
                                'tempdata',
                                'tempus_challenge_dag',
                                'csv')
        return csv_path

    @patch('pandas.read_json', autospec=True)
    def test_transform_key_headlines_to_csv_convert_sucess(self,
                                                           file_reader_func,
                                                           home_directory_res):
        """call to flatten jsons in the tempus_bonus_challenge_dag headline
        folder succeeds."""

        # Arrange
        # name of the pipeline under test
        pipeline_name = "tempus_bonus_challenge_dag"

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        tf_func = c.TransformOperations.transform_key_headlines_to_csv
        extract_func = c.ExtractOperations.extract_news_data_from_dataframe

        # create the dummy input data that will be passed to the function
        # under test
        dummy_json_file = None
        transform_data_df = MagicMock(spec=pandas.DataFrame)

        # setup a Mock of the extract and transform function dependencies
        tf_func_mock = MagicMock(spec=tf_func)
        extract_func_mock = MagicMock(spec=extract_func)

        # define the mock function behaviors when called
        extract_func_mock.side_effect = lambda data: "extracted data"
        tf_func_mock.side_effect = lambda data: transform_data_df
        file_reader_func.side_effect = lambda data: "read-in json file"

        # path to the fake csv directory the function under test
        # uses
        csv_dir = os.path.join(home_directory_res,
                               'tempdata',
                               pipeline_name,
                               'csv')

        # name and path to the file that will be created after transformation
        filename = str(datetime.datetime.now()) + "_" + "sample.csv"
        # fp = os.path.join(csv_dir, filename)

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and place the dummy csv files
            # in that directory to test the method
            patcher.fs.create_dir(csv_dir)

            # calling the transformed DataFrame's to_csv() creates a new
            # csv file in the fake directory
            transform_data_df.to_csv.side_effect = patcher.fs.create_file

        # Act
            result = tf_func(dummy_json_file,
                             filename,
                             file_reader_func,
                             extract_func_mock,
                             tf_func_mock)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        # return status of the operation should be True to indicate success
        assert result is True

    @patch('pandas.read_json', autospec=True)
    def test_transform_jsons_to_dataframe_merger_succeeds(self,
                                                          file_reader_func):
        """merging a set of transformed DataFrames from jsons succeeds."""

        # Arrange

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        tf_func = c.TransformOperations.transform_jsons_to_dataframe_merger
        extract_func = c.ExtractOperations.extract_news_data_from_dataframe
        data_to_df_func = c.TransformOperations.transform_data_to_dataframe

        # create the dummy input data that will be passed to the function
        # under test
        json_files = ['file1.json', 'file2.json']
        data_df1 = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'],
                                 'B': ['B0', 'B1', 'B2', 'B3'],
                                 'C': ['C0', 'C1', 'C2', 'C3'],
                                 'D': ['D0', 'D1', 'D2', 'D3']},
                                index=[0, 1, 2, 3])

        data_df2 = pd.DataFrame({'A': ['A4', 'A5', 'A6', 'A7'],
                                 'B': ['B4', 'B5', 'B6', 'B7'],
                                 'C': ['C4', 'C5', 'C6', 'C7'],
                                 'D': ['D4', 'D5', 'D6', 'D7']},
                                index=[4, 5, 6, 7])

        # setup a Mock of the extract and transform function dependencies
        tf_func_mock = MagicMock(spec=data_to_df_func)
        extract_func_mock = MagicMock(spec=extract_func)

        # define the mock function behaviors when called
        extract_func_mock.side_effect = lambda data: "extracted data"
        tf_func_mock.side_effect = [data_df1, data_df2]
        file_reader_func.side_effect = lambda data: "read-in json file"

        # Act
        result = tf_func(json_files,
                         file_reader_func,
                         extract_func_mock,
                         tf_func_mock)

        # Assert
        # an empty dataframe is expected since three empty dataframes were
        # merged together from the three empty json files.
        expected_dataframe = pd.concat([data_df1, data_df2])
        assert expected_dataframe.equals(result)

    def test_helper_execute_json_transformation_for_one_json_succeeds(self):
        """transforming a set of jsons in a valid directory succeeds"""

        # Arrange
        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        transfm_fnc = c.TransformOperations.helper_execute_json_transformation
        js_csv_fnc = c.TransformOperations.transform_news_headlines_json_to_csv
        js_df_fnc = c.TransformOperations.transform_jsons_to_dataframe_merger
        h_csv_fnc = c.TransformOperations.transform_headlines_dataframe_to_csv

        # Mock out the functions that the function under test uses
        json_csv_func = MagicMock(spec=js_csv_fnc)
        jsons_df_func = MagicMock(spec=js_df_fnc)
        df_csv_func = MagicMock(spec=h_csv_fnc)

        # Mock out the behavior of the function under test, returns True
        # indicating the single json file passed in was successfully
        # converted to a csv
        json_csv_func.side_effect = lambda files, filename, reader: True

        # setup pipeline information
        pipeline_name = "tempus_challenge_dag"

        headline_dir = os.path.join('tempdata',
                                    pipeline_name,
                                    'headlines')

        # create dummy json file
        file_path = os.path.join(headline_dir, 'dummy.json')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory containing one json file
            # to test the method
            patcher.fs.create_dir(headline_dir)
            patcher.fs.create_file(file_path)

        # Act
            # function should raise errors on an empty directory
            result = transfm_fnc(directory=headline_dir,
                                 json_to_csv_func=json_csv_func,
                                 jsons_to_df_func=jsons_df_func,
                                 df_to_csv_func=df_csv_func)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        # the transformation of the one json file succeeds and status returned
        # is True
        assert result is True

    def test_helper_execute_json_transformation_for_three_jsons_succeeds(self):
        """transforming a set of jsons in a valid directory succeeds"""

        # Arrange
        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        transfm_fnc = c.TransformOperations.helper_execute_json_transformation
        js_csv_fnc = c.TransformOperations.transform_news_headlines_json_to_csv
        js_df_fnc = c.TransformOperations.transform_jsons_to_dataframe_merger
        h_csv_fnc = c.TransformOperations.transform_headlines_dataframe_to_csv

        # Mock out the functions that the function under test uses
        json_csv_func = MagicMock(spec=js_csv_fnc)
        jsons_df_func = MagicMock(spec=js_df_fnc)
        df_csv_func = MagicMock(spec=h_csv_fnc)

        # Mock out the behavior of the function under test, returns True
        # indicating the single json file passed in was successfully
        # converted to a csv
        json_csv_func.side_effect = lambda files, filename, reader: True
        jsons_df_func.side_effect = lambda files, reader: pd.DataFrame()
        df_csv_func.side_effect = lambda dataframe, filename: True

        # setup pipeline information
        pipeline_name = "tempus_challenge_dag"

        headline_dir = os.path.join('tempdata',
                                    pipeline_name,
                                    'headlines')

        # create three dummy json files
        file_path_one = os.path.join(headline_dir, 'dummy1.json')
        file_path_two = os.path.join(headline_dir, 'dummy2.json')
        file_path_three = os.path.join(headline_dir, 'dummy3.json')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory containing three json file
            # to test the method
            patcher.fs.create_dir(headline_dir)
            patcher.fs.create_file(file_path_one)
            patcher.fs.create_file(file_path_two)
            patcher.fs.create_file(file_path_three)

        # Act
            # function should raise errors on an empty directory
            result = transfm_fnc(directory=headline_dir,
                                 json_to_csv_func=json_csv_func,
                                 jsons_to_df_func=jsons_df_func,
                                 df_to_csv_func=df_csv_func)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        # the transformation of the one json file succeeds and status returned
        # is True
        assert result is True

    def test_helper_execute_keyword_json_transformation_succeeds(self):
        """transformation of all files in a set of json files in a directory
        succeeds.
        """

        # Arrange
        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        tfnc = c.TransformOperations.helper_execute_keyword_json_transformation
        js_csv_fnc = c.TransformOperations.transform_key_headlines_to_csv

        # Mock out the functions that the function under test uses
        headlines_to_csv_func = MagicMock(spec=js_csv_fnc)

        # setup behavior of the function - two of the json files get converted
        # to csv while transformation of the third fails
        headlines_to_csv_func.side_effect = [True, True, True]

        pipeline_name = "tempus_challenge_dag"

        headline_dir = os.path.join('tempdata',
                                    pipeline_name,
                                    'headlines')

        # create dummy non-json files
        file_path_one = os.path.join(headline_dir,
                                     'my_stuff1_headlines.json')
        file_path_two = os.path.join(headline_dir,
                                     'my_stuff2_headlines.json')
        file_path_three = os.path.join(headline_dir,
                                       'my_stuff3_headlines.json')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            # patcher.fs.create_dir(headline_dir)
            patcher.fs.create_file(file_path_one)
            patcher.fs.create_file(file_path_two)
            patcher.fs.create_file(file_path_three)

        # Act
            # function should raise errors on an empty directory
            result = tfnc(directory=headline_dir,
                          json_transfm_func=headlines_to_csv_func)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        # expected a True result since all three json files were transformed
        # to csv
        assert result is True

    def test_transform_data_to_dataframe_succeeds(self):
        """conversion of a dictionary of numpy array news data into
        a Pandas Dataframe succeed"""

        # Arrange

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        tf_func = c.TransformOperations.transform_data_to_dataframe

        # craft the dummy extracted news data
        extracted_data = {'source_id': ["wired"],
                          'source_name': ["Wired"],
                          'author': ["Klint Finley"],
                          'title': ["Microsoft Calls a Truce in Patent Wars"],
                          'description': ["The software giant, whose\
                          former CEO once called\
                          Linux a \\\"cancer,\\\" will let others use 60,000\
                          patents for Linux-related open source projects."],
                          'url': ["https://www.wired.com/story/microsoft-ca\
                          lls-truce-in-linux-patent-wars/"],
                          'url_to_image': ["https://media.wired.com/photos/5bb\
                          e9c0f2b915f2dff96d6f4/191:100/pass/Satya-Microsoft-M\
                          ichelleG.jpg"],
                          'published_at': ["2018-10-11T23:33:03Z"],
                          'content': ["Microsoft is calling for a truce in the\
                           patent war. This week the company said it will\
                            allow more than 2,600 other companies, including\
                             traditional rivals like Google and IBM, to use\
                              the technology behind 60,000 Microsoft patents\
                               for their own Linux related o… [+4435 chars]"]}

        # Act
        result_df = tf_func(extracted_data)

        # Assert
        # because the dataframe has multiple columns which might be all tedious
        # verifying in a single test case (according to some testing
        # best-practices, ideally one test case should have just one assert
        # statement), we verify the value of just three columns: source_id,
        # source_name, and title.
        # The three asserts are coherent in this particular instance, since
        # they evaluate parts of the same returned object.
        actual_source_id = result_df['news_source_id'][0]
        actual_source_name = result_df['news_source_name'][0]
        actual_title = result_df['news_title'][0]

        # verify the actual result with expectations
        assert "wired" in actual_source_id
        assert "Wired" in actual_source_name
        assert "Microsoft Calls a Truce in Patent Wars" in actual_title

    def test_transform_headlines_to_csv_pipelineone_success(self,
                                                            airflow_context,
                                                            headline_dir_res):
        """call to flatten jsons in the 'tempus_challenge_dag' headline
        folder succeeds."""

        # Arrange

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        # get the current pipeline info
        tf_json_func = c.TransformOperations.helper_execute_json_transformation
        j_fn = c.TransformOperations.helper_execute_keyword_json_transformation
        transfm_fnc = c.TransformOperations.transform_headlines_to_csv

        # setup a Mock of the transform function dependencies
        tf_json_func_mock = MagicMock(spec=tf_json_func)
        tf_keyword_func_mock = MagicMock(spec=j_fn)
        pipeline_info_obj = MagicMock(spec=c.NewsInfoDTO)
        news_info_obj = MagicMock(spec=c.NewsInfoDTO)

        # setup the behaviors of these Mocks
        tf_json_func_mock.side_effect = lambda dir, exec_date: True
        tf_keyword_func_mock.side_effect = lambda dir, exec_date: None
        pipeline_info_obj.side_effect = lambda pipeline_name: news_info_obj
        news_info_obj.get_headlines_directory = headline_dir_res

        # create three dummy json files
        file_path_one = os.path.join(headline_dir_res, 'dummy1.json')
        file_path_two = os.path.join(headline_dir_res, 'dummy2.json')
        file_path_three = os.path.join(headline_dir_res, 'dummy3.json')

        # setup a fake headlines directory which the function under test
        # requires be already existent
        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            patcher.fs.create_dir(headline_dir_res)
            patcher.fs.create_file(file_path_one)
            patcher.fs.create_file(file_path_two)
            patcher.fs.create_file(file_path_three)

        # Act
            result = transfm_fnc(pipeline_information=pipeline_info_obj,
                                 tf_json_func=tf_json_func_mock,
                                 tf_key_json_func=tf_keyword_func_mock,
                                 **airflow_context)

        # Assert
        # return status of the transformation operation should be True to
        # indicate success
        assert result is True

    def test_transform_headlines_to_csv_pipelinetwo_success(self,
                                                            headline_dir_res,
                                                            airflow_context):
        """call to flatten jsons in the 'tempus_bonus_challenge_dag' headline
        folder succeeds."""

        # Arrange

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        # get the current pipeline info
        tf_json_func = c.TransformOperations.helper_execute_json_transformation
        j_fn = c.TransformOperations.helper_execute_keyword_json_transformation
        transfm_fnc = c.TransformOperations.transform_headlines_to_csv

        # setup a Mock of the transform function dependencies
        tf_json_func_mock = MagicMock(spec=tf_json_func)
        tf_keyword_func_mock = MagicMock(spec=j_fn)
        pipeline_info_obj = MagicMock(spec=c.NewsInfoDTO)
        news_info_obj = MagicMock(spec=c.NewsInfoDTO)

        # setup the behaviors of these Mocks
        tf_json_func_mock.side_effect = lambda dir, exec_date: None
        tf_keyword_func_mock.side_effect = lambda dir, exec_date: True
        pipeline_info_obj.side_effect = lambda pipeline_name: news_info_obj

        # setup the information about the active pipeline
        pipeline_name = 'tempus_bonus_challenge_dag'
        airflow_context['dag'].dag_id = pipeline_name
        headlines_directory = os.path.join('tempdata',
                                           pipeline_name,
                                           'headlines')

        news_info_obj.get_headlines_directory = headlines_directory

        # create three dummy json files
        file_path_one = os.path.join(headlines_directory, 'dummy1.json')
        file_path_two = os.path.join(headlines_directory, 'dummy2.json')
        file_path_three = os.path.join(headlines_directory, 'dummy3.json')

        # setup a fake headlines directory which the function under test
        # requires be already existent
        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            patcher.fs.create_dir(headlines_directory)
            patcher.fs.create_file(file_path_one)
            patcher.fs.create_file(file_path_two)
            patcher.fs.create_file(file_path_three)

        # Act
            result = transfm_fnc(pipeline_information=pipeline_info_obj,
                                 tf_json_func=tf_json_func_mock,
                                 tf_key_json_func=tf_keyword_func_mock,
                                 **airflow_context)

        # Assert
        # return status of the transformation operation should be True to
        # indicate success
        assert result is True

    @patch('pandas.read_json', autospec=True)
    def test_transform_news_headlines_one_json_to_csv_succeeds(self,
                                                               reader_func,
                                                               csv_dir_res):
        """transform of a single news headline json file to csv succeeds."""

        # Arrange

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        # get the current pipeline info
        transform_func = c.TransformOperations.transform_data_to_dataframe
        extract_func = c.ExtractOperations.extract_news_data_from_dataframe
        trnsfm_fnc = c.TransformOperations.transform_news_headlines_json_to_csv

        # setup a Mock of the transform function dependencies
        transform_func_func_mock = MagicMock(spec=transform_func)
        extract_func_mock = MagicMock(spec=extract_func)
        transformed_data_df = MagicMock(spec=pandas.DataFrame)

        # setup the behaviors of these Mocks
        extract_func_mock.side_effect = lambda data: "extracted data"
        transform_func_func_mock.side_effect = lambda data: transformed_data_df
        reader_func.side_effect = lambda file: "success reading json"

        # create one dummy json file
        file_path = os.path.join(csv_dir_res, 'dummy.json')

        # setup a fake headlines directory which the function under test
        # requires be already existent
        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            patcher.fs.create_dir(csv_dir_res)
            patcher.fs.create_file(file_path)

            # calling the transformed DataFrame's to_csv() creates a new
            # csv file in the fake directory
            transformed_data_df.to_csv.side_effect = patcher.fs.create_file

        # Act
            result = trnsfm_fnc(file_path,
                                "dummy.csv",
                                extract_func=extract_func_mock,
                                transform_func=transform_func_func_mock,
                                read_js_func=reader_func)

        # Assert
        # return status of the transformation operation should be True to
        # indicate success
        assert result[0] is True

    @patch('pandas.read_json', autospec=True)
    def test_transform_news_headlines_one_json_to_csv_fails(self,
                                                            reader_func,
                                                            csv_dir_res):
        """transform of a single news headline json file to csv fails."""

        # Arrange

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        # get the current pipeline info
        transform_func = c.TransformOperations.transform_data_to_dataframe
        extract_func = c.ExtractOperations.extract_news_data_from_dataframe
        trnsfm_fnc = c.TransformOperations.transform_news_headlines_json_to_csv

        # setup a Mock of the transform function dependencies
        transform_func_func_mock = MagicMock(spec=transform_func)
        extract_func_mock = MagicMock(spec=extract_func)
        transformed_data_df = MagicMock(spec=pandas.DataFrame)

        # setup the behaviors of these Mocks
        extract_func_mock.side_effect = lambda data: "extracted data"
        transform_func_func_mock.side_effect = lambda data: transformed_data_df
        reader_func.side_effect = lambda file: "success reading json"

        # create one dummy json file
        file_path = os.path.join(csv_dir_res, 'dummy.json')

        # setup a fake headlines directory which the function under test
        # requires be already existent
        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            patcher.fs.create_dir(csv_dir_res)

            # calling the transformed DataFrame's to_csv() fails and
            # doesn't created a new file in the fake directory
            transformed_data_df.to_csv.side_effect = lambda *args: None

        # Act
            result = trnsfm_fnc(file_path,
                                "dummy.csv",
                                extract_func=extract_func_mock,
                                transform_func=transform_func_func_mock,
                                read_js_func=reader_func)

        # Assert
        # return status of the transformation operation should be False to
        # indicate failure - the csv file could not be created as no csv
        # directory exists
        assert result[0] is False

    def test_transform_news_headlines_json_to_csv_no_articles_fails(self):
        """function behaves properly in the absence of news articles."""

        # Arrange

        # name of the pipeline under test
        pipeline_name = "tempus_challenge_dag"

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        # get the current pipeline info
        tf_func = c.TransformOperations.transform_key_headlines_to_csv
        extract_func = c.ExtractOperations.extract_news_data_from_dataframe

        # create the dummy input data that will be passed to the function
        # under test
        dummy_json_file = None
        transform_data_df = MagicMock(spec=pandas.DataFrame)

        # setup a Mock of the extract and transform function dependencies
        tf_func_mock = MagicMock(spec=tf_func)
        extract_func_mock = MagicMock(spec=extract_func)
        file_reader_func = MagicMock(spec=pandas.read_json)

        # an empty dictionary returned indicates to the function under
        # test that no news articles were found in the json file
        extract_func_mock.side_effect = lambda data: {}
        tf_func_mock.side_effect = lambda data: transform_data_df
        file_reader_func.side_effect = lambda data: "read-in json file"

        # calling the transformed DataFrame's to_csv() creates a new
        # csv file in the fake directory
        transform_data_df.to_csv.side_effect = lambda filepath: "no file"

        # name and path to the file that will be created after transformation
        filename = str(datetime.datetime.now()) + "_" + "sample.csv"

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # path to the fake csv directory the function under test
            # uses
            csv_dir = os.path.join('tempdata',
                                   pipeline_name,
                                   'csv')

            # create a fake filesystem directory and place the dummy csv files
            # in that directory to test the method
            patcher.fs.create_dir(csv_dir)

        # Act
            result = tf_func(dummy_json_file,
                             filename,
                             file_reader_func,
                             extract_func_mock,
                             tf_func_mock)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        # No news articles is a valid state of the function and returns
        # True. Inspecting the function's returned status message verifies
        # that indeed no news article was found
        assert result[0] is True
        assert "No News articles found" in result[1]

    @patch('pandas.read_json', autospec=True)
    def test_transform_key_headlines_to_csv_convert_fails(self,
                                                          file_reader_func,
                                                          home_directory_res):
        """call to flatten jsons in the tempus_bonus_challenge_dag headline
        folder fails."""

        # Arrange
        # name of the pipeline under test
        pipeline_name = "tempus_bonus_challenge_dag"

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        # get the current pipeline info
        tf_func = c.TransformOperations.transform_key_headlines_to_csv
        extract_func = c.ExtractOperations.extract_news_data_from_dataframe

        # create the dummy input data that will be passed to the function
        # under test
        dummy_json_file = None
        transform_data_df = MagicMock(spec=pandas.DataFrame)

        # setup a Mock of the extract and transform function dependencies
        tf_func_mock = MagicMock(spec=tf_func)
        extract_func_mock = MagicMock(spec=extract_func)
        extract_func_mock.side_effect = lambda data: "extracted data"
        tf_func_mock.side_effect = lambda data: transform_data_df
        file_reader_func.side_effect = lambda data: "read-in json file"

        # path to the fake csv directory the function under test
        # uses
        csv_dir = os.path.join(home_directory_res,
                               'tempdata',
                               pipeline_name,
                               'csv')

        # name and path to the file that will be created after transformation
        filename = str(datetime.datetime.now()) + "_" + "sample.csv"
        # fp = os.path.join(csv_dir, filename)

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and place the dummy csv files
            # in that directory to test the method
            patcher.fs.create_dir(csv_dir)

            # calling the transformed DataFrame's to_csv() creates a new
            # csv file in the fake directory
            transform_data_df.to_csv.side_effect = lambda filepath: "no file"

        # Act
            result = tf_func(dummy_json_file,
                             filename,
                             file_reader_func,
                             extract_func_mock,
                             tf_func_mock)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        # return status of the operation should be False to indicate the
        # csv file was not saved to the directory
        assert result[0] is False

    def test_transform_headlines_to_csv_wrong_pipeline_fails(self,
                                                             airflow_context,
                                                             headline_dir_res):
        """flattening of a set of json files to csv fails when a
        non-existent DAG pipeline name is used.
        """

        # Arrange

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        tf_json_func = c.TransformOperations.helper_execute_json_transformation
        j_fn = c.TransformOperations.helper_execute_keyword_json_transformation
        transfm_fnc = c.TransformOperations.transform_headlines_to_csv

        info_obj = c.NewsInfoDTO

        # setup a Mock of the transform function dependencies
        tf_json_func_mock = MagicMock(spec=tf_json_func)
        tf_keyword_json_func_mock = MagicMock(spec=j_fn)

        # setup the dummy NewsInfoDTO class that, on intialization
        # acquires information about a pipeline-name passed in.
        # when it is first called, with a pipeline name, it initializes
        # various properties and returns an instance.
        pipeline_info_obj = MagicMock(spec=info_obj)
        news_info_obj = MagicMock(spec=info_obj)

        # setup the behaviors of these Mocks
        tf_json_func_mock.side_effect = lambda dir, exec_date: None
        tf_keyword_json_func_mock.side_effect = lambda dir, exec_date: None
        pipeline_info_obj.side_effect = lambda pipeline_name: news_info_obj

        # use the pytest resource representing an airflow context object
        airflow_context['dag'].dag_id = "non_existent_pipeline_name"

        # setup a dummy class as a Mock object initializing the property
        # that transform_headlines_to_csv() exercises
        news_info_obj.get_headlines_directory = "/dummy/dir/headlines"

        # Act
        # on calling the function with a wrong pipeline name it fails,
        # returning False to signal a failed status
        result = transfm_fnc(pipeline_information=pipeline_info_obj,
                             transform_json_fnc=tf_json_func_mock,
                             transform_key_json_fnc=tf_keyword_json_func_mock,
                             **airflow_context)

        # Assert
        assert result is False

    def test_helper_execute_json_transformation_empty_dir_fails(self):
        """transforming a set of jsons in an empty directory fails."""

        # Arrange
        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        transfm_fnc = c.TransformOperations.helper_execute_json_transformation
        js_csv_fnc = c.TransformOperations.transform_news_headlines_json_to_csv
        js_df_fnc = c.TransformOperations.transform_jsons_to_dataframe_merger
        h_csv_fnc = c.TransformOperations.transform_headlines_dataframe_to_csv

        # Mock out the functions that the function under test uses
        json_csv_func = MagicMock(spec=js_csv_fnc)
        jsons_df_func = MagicMock(spec=js_df_fnc)
        df_csv_func = MagicMock(spec=h_csv_fnc)

        pipeline_name = "tempus_challenge_dag"

        headline_dir = os.path.join('tempdata',
                                    pipeline_name,
                                    'headlines')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem empty directory to test the method
            patcher.fs.create_dir(headline_dir)

        # Act
            # function should raise errors on an empty directory
            with pytest.raises(FileNotFoundError) as err:
                transfm_fnc(directory=headline_dir,
                            json_to_csv_func=json_csv_func,
                            jsons_to_df_func=jsons_df_func,
                            df_to_csv_func=df_csv_func)

            actual_message = str(err.value)
            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert "Directory is empty" in actual_message

    def test_helper_execute_json_transformation_no_jsons_in_dir_fails(self):
        """transforming a set of jsons in a non-empty directory but having no
        json files fails.
        """

        # Arrange
        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        transfm_fnc = c.TransformOperations.helper_execute_json_transformation
        js_csv_fnc = c.TransformOperations.transform_news_headlines_json_to_csv
        js_df_fnc = c.TransformOperations.transform_jsons_to_dataframe_merger
        h_csv_fnc = c.TransformOperations.transform_headlines_dataframe_to_csv

        # Mock out the functions that the function under test uses
        json_csv_func = MagicMock(spec=js_csv_fnc)
        jsons_df_func = MagicMock(spec=js_df_fnc)
        df_csv_func = MagicMock(spec=h_csv_fnc)

        pipeline_name = "tempus_challenge_dag"

        headline_dir = os.path.join('tempdata',
                                    pipeline_name,
                                    'headlines')

        # create dummy non-json files
        file_path_one = os.path.join(headline_dir, 'stuff1.txt')
        file_path_two = os.path.join(headline_dir, 'stuff2.rtf')
        file_path_three = os.path.join(headline_dir, 'stuff3.doc')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            # patcher.fs.create_dir(headline_dir)
            patcher.fs.create_file(file_path_one, contents='dummy txt')
            patcher.fs.create_file(file_path_two, contents='dummy rtf')
            patcher.fs.create_file(file_path_three, contents='dummy doc')

        # Act
            # function should raise errors on an empty directory
            with pytest.raises(FileNotFoundError) as err:
                transfm_fnc(directory=headline_dir,
                            json_to_csv_func=json_csv_func,
                            jsons_to_df_func=jsons_df_func,
                            df_to_csv_func=df_csv_func)

            actual_message = str(err.value)
            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert "Directory has no json-headline files" in actual_message

    def test_helper_execute_keyword_json_transformation_no_jsons_fails(self):
        """transforming a set of jsons in a non-empty directory but having no
        json files fails.
        """

        # Arrange
        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        tfnc = c.TransformOperations.helper_execute_keyword_json_transformation
        js_csv_fnc = c.TransformOperations.transform_key_headlines_to_csv

        # Mock out the functions that the function under test uses
        headlines_to_csv_func = MagicMock(spec=js_csv_fnc)

        pipeline_name = "tempus_challenge_dag"

        headline_dir = os.path.join('tempdata',
                                    pipeline_name,
                                    'headlines')

        # create dummy non-json files
        file_path_one = os.path.join(headline_dir, 'stuff1.txt')
        file_path_two = os.path.join(headline_dir, 'stuff2.rtf')
        file_path_three = os.path.join(headline_dir, 'stuff3.doc')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            # patcher.fs.create_dir(headline_dir)
            patcher.fs.create_file(file_path_one, contents='dummy txt')
            patcher.fs.create_file(file_path_two, contents='dummy rtf')
            patcher.fs.create_file(file_path_three, contents='dummy doc')

        # Act
            # function should raise errors on an empty directory
            with pytest.raises(FileNotFoundError) as err:
                tfnc(directory=headline_dir,
                     json_transfm_func=headlines_to_csv_func)

            actual_message = str(err.value)
            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert "Directory has no json-headline files" in actual_message

    def test_helper_execute_keyword_json_transformation_partialfile_fail(self):
        """partial transformation of some files in a set of json files within a
        directory fails.
        """

        # Arrange
        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        tfnc = c.TransformOperations.helper_execute_keyword_json_transformation
        js_csv_fnc = c.TransformOperations.transform_key_headlines_to_csv

        # Mock out the functions that the function under test uses
        headlines_to_csv_func = MagicMock(spec=js_csv_fnc)

        # setup behavior of the function - two of the json files get converted
        # to csv while transformation of the third fails
        headlines_to_csv_func.side_effect = [True, True, False]

        pipeline_name = "tempus_challenge_dag"

        headline_dir = os.path.join('tempdata',
                                    pipeline_name,
                                    'headlines')

        # create dummy non-json files
        file_path_one = os.path.join(headline_dir,
                                     'my_stuff1_headlines.json')
        file_path_two = os.path.join(headline_dir,
                                     'my_stuff2_headlines.json')
        file_path_three = os.path.join(headline_dir,
                                       'my_stuff3_headlines.json')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            # patcher.fs.create_dir(headline_dir)
            patcher.fs.create_file(file_path_one)
            patcher.fs.create_file(file_path_two)
            patcher.fs.create_file(file_path_three)

        # Act
            # function should raise errors on an empty directory
            result = tfnc(directory=headline_dir,
                          json_transfm_func=headlines_to_csv_func)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        # expected a False result since only two of the three json files were
        # transformed to csv
        assert result is False

    def test_transform_data_to_dataframe_fails(self):
        """conversion of a dictionary of news data into
        a Pandas Dataframe fails"""

        # Arrange

        # Function Aliases
        # use an alias since the length of the real function call when used
        # is more than PEP-8's 79 line-character limit.
        tf_func = c.TransformOperations.transform_data_to_dataframe

        # function fails when the incoming news data is empty
        data = None

        # Act
        with pytest.raises(ValueError) as err:
            tf_func(data)

        # Assert
        actual_message = str(err.value)
        assert "news data argument cannot be empty" in actual_message
