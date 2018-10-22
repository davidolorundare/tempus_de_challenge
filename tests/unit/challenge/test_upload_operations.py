"""Tempus Data Engineer Challenge  - Unit Tests.

Defines unit tests for the underlining functions for
the uploading to the Amazon S3 the transformed news data
task performed in the DAGs.
"""

import boto3
import botocore
import datetime
import os
import pytest

from unittest.mock import MagicMock
from moto import mock_s3

from airflow.models import DAG

from dags import challenge as c

from pyfakefs.fake_filesystem_unittest import Patcher


@pytest.mark.uploadtests
class TestUploadOperations:
    """test the functions for task to upload csvs to Amazon S3."""

    @pytest.fixture(scope='class')
    def home_directory_res(self) -> str:
        """returns a pytest resource - path to the Airflow Home directory."""
        return str(os.environ['HOME'])

    @pytest.fixture(scope='class')
    def airflow_context(self) -> dict:
        """returns an airflow context object for tempus_challenge_dag.

        Mimics parts of the airflow context returned during execution
        of the tempus_challenge_dag.

        https://airflow.apache.org/code.html#default-variables
        """

        dag = MagicMock(spec=DAG)
        dag.dag_id = "tempus_challenge_dag"

        return {
            'ds': datetime.datetime.now().isoformat().split('T')[0],
            'dag': dag
        }

    @pytest.fixture(scope='class')
    def bucket_names(self) -> list:
        """returns the list of available Amazon S3 buckets."""

        return ['tempus-challenge-csv-headlines',
                'tempus-bonus-challenge-csv-headlines']

    @pytest.fixture(scope='function')
    def setup_s3_bucket_res(self,
                            bucket_name,
                            create_bucket=True,
                            endpoint=None) -> tuple:
        """configure s3 resource and client objects."""

        # create an s3 service client object
        s3_service_client = boto3.client('s3', region_name='us-east-1',
                                         aws_access_key_id="fake_key",
                                         aws_secret_access_key="fake_scrt",
                                         endpoint_url=endpoint)

        # create an s3 resource object
        try:
            s3_resource_obj = boto3.resource('s3', region_name='us-east-1',
                                             aws_access_key_id="fake_key",
                                             aws_secret_access_key="fake_scrt",
                                             endpoint_url=endpoint)

            s3_resource_obj.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError:
            pass
        else:
            err_message = "{bucket} shouldn't exist".format(bucket=bucket_name)
            raise EnvironmentError(err_message)

        # create a fake s3 bucket
        if create_bucket:
            s3_resource_obj.create_bucket(Bucket=bucket_name)

        # setup returns the s3 client and resource objects
        return s3_service_client, s3_resource_obj

    @pytest.fixture(scope='class')
    def teardown_s3_bucket_res(self, bucket_name):
        """clean up the bucket and its objects"""
        bucket = boto3.resource('s3').Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.delete()

    @mock_s3
    def test_upload_csv_to_s3_files_upload_correctly(self,
                                                     airflow_context,
                                                     bucket_names,
                                                     home_directory_res):
        """tests call to boto library to upload a file is actually made
        and return a correct status.
        """

        # Arrange
        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # name of the S3 bucket
        bucket_name = bucket_names[0]

        # create the S3 client and resource objects required
        client_obj, resource_obj = self.setup_s3_bucket_res(bucket_name)

        # setup a Mock of the boto3 resources and file upload functions
        # upload_client = MagicMock(spec=boto3.client('s3'))
        # resource_client = MagicMock(spec=boto3.resource('s3'))
        # upload_client.upload_file.side_effect = lambda fname,
        # bname, key: None
        # resource_client.buckets.all.side_effect = lambda: bucket_names

        # path to the fake news and csv directories the function under test
        # uses
        csv_dir = os.path.join(home_directory_res,
                               'tempdata',
                               pipeline_name,
                               'csv')

        news_dir = os.path.join(home_directory_res,
                                'tempdata',
                                pipeline_name,
                                'news')

        # create dummy csv files that will be uploaded by the function
        file_path = os.path.join(csv_dir, 'stuff.csv')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and place the dummy csv files
            # in that directory to test the method
            patcher.fs.create_dir(csv_dir)
            patcher.fs.create_dir(news_dir)
            patcher.fs.create_file(file_path, contents='1,dummy,txt')

        # Act
            # attempt uploading a file to a valid s3 bucket
            stat, msg = c.UploadOperations.upload_csv_to_s3(csv_dir,
                                                            bucket_name,
                                                            client_obj,
                                                            resource_obj,
                                                            **airflow_context)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # clean up the s3 bucket and objects
        self.teardown_s3_bucket_res(bucket_name)

        # Assert

        # ensure the boto3 upload_file() function was called with correct
        # arguments, resulting outcome should be True from the function
        assert stat is True
        assert "upload successful" in msg

    @mock_s3
    def test_upload_csv_to_s3_integration_succeeds(self,
                                                   airflow_context,
                                                   bucket_names,
                                                   home_directory_res):
        """Integration test - function uploads files successfully
        when interacting with a fake Amazon API from the moto library.
        """

        # Arrange
        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # name of the Amazon S3 bucket that will be created
        bucket_name = bucket_names[0]

        # create the s3 client and resource objects required
        client_obj, resource_obj = self.setup_s3_bucket_res(bucket_name)

        # track the upload status and the buckets contents before and after
        # the upload operation
        stat = None
        msg = None
        bucket_contents_before_upload = None
        bucket_contents_after_upload = None

        # create a fake filesystem directory from which to upload the csv
        # files to s3
        csv_dir = os.path.join(home_directory_res,
                               'tempdata',
                               pipeline_name,
                               'csv')

        news_dir = os.path.join(home_directory_res,
                                'tempdata',
                                pipeline_name,
                                'news')

        # create the path to the dummy csv files that will be uploaded
        # by the function call
        upload_path_one = os.path.join(csv_dir, 'stuff1.csv')
        upload_path_two = os.path.join(csv_dir, 'stuff2.csv')
        upload_path_three = os.path.join(csv_dir, 'stuff3.csv')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            patcher.fs.create_dir(csv_dir)
            patcher.fs.create_dir(news_dir)

            # place the dummy csv files in that directory to test the method
            patcher.fs.create_file(upload_path_one, contents='1,dummy,txt')
            patcher.fs.create_file(upload_path_two, contents='2,dummy,txt')
            patcher.fs.create_file(upload_path_three, contents='3,dummy,txt')

        # Act
            # access the created bucket and verify that the bucket is really
            # empty - its length should be 0 before the function call
            bucket = resource_obj.Bucket(bucket_name)
            bucket_contents_before_upload = len(list(bucket.objects.all()))

            # Perform the upload operation
            stat, msg = c.UploadOperations.upload_csv_to_s3(csv_dir,
                                                            bucket_name,
                                                            client_obj,
                                                            resource_obj,
                                                            **airflow_context)

            # verify the uploaded files now exist in the bucket, we just
            # uploaded three files, so the number of objects in the bucket
            # now should be three
            bucket_contents_after_upload = len(list(bucket.objects.all()))

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # clean up the s3 bucket
        self.teardown_s3_bucket_res(bucket_name)

        # Assert

        # confirm that before uploading the bucket is empty and after uploading
        # there are three files
        assert bucket_contents_before_upload == 0
        assert bucket_contents_after_upload == 3

        # upload function returns True if it was called
        assert stat is True
        assert "upload successful" in msg

    @pytest.mark.skip(reason="requires FakeS3 server to be setup and running")
    def test_upload_csv_to_s3_fakes3_integration_succeeds(self,
                                                          airflow_context,
                                                          bucket_names,
                                                          home_directory_res):
        """Integration Test - csv files upload successfully to a live
        S3 Mock server.
        """

        # Arrange
        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # setup a fake Amazon S3 server using Moto, creating a fake S3 bucket
        bucket_name = bucket_names[0]

        # FakeS3 Server Endpoint
        fake_url = "http://localhost:4567"

        # create s3 client and resource objects
        client_obj, resource_obj = self.setup_s3_bucket_res(bucket_name,
                                                            endpoint=fake_url)

        # track the upload status and the buckets contents before and after
        # the upload operation
        status = None
        bucket_contents_before_upload = None
        bucket_contents_after_upload = None

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory from which to upload the csv
            # files to s3
            csv_dir = os.path.join(home_directory_res,
                                   'tempdata',
                                   pipeline_name,
                                   'csv')

            news_dir = os.path.join(home_directory_res,
                                    'tempdata',
                                    pipeline_name,
                                    'news')

            # create the path to the dummy csv files that will be uploaded
            # by the function call
            upload_path_one = os.path.join(csv_dir, 'stuff1.csv')
            upload_path_two = os.path.join(csv_dir, 'stuff2.csv')
            upload_path_three = os.path.join(csv_dir, 'stuff3.csv')

            # place the dummy csv files in that directory to test the method
            patcher.fs.create_dir(csv_dir)
            patcher.fs.create_dir(news_dir)
            patcher.fs.create_file(upload_path_one, contents='1,dummy,txt')
            patcher.fs.create_file(upload_path_two, contents='2,dummy,txt')
            patcher.fs.create_file(upload_path_three, contents='3,dummy,txt')

        # Act
            # access the created bucket and verify that the bucket is really
            # empty - its length should be 0 before the function call
            bucket = resource_obj.Bucket(bucket_name)
            bucket_contents_before_upload = len(list(bucket.objects.all()))

            # Perform the upload operation
            status = c.UploadOperations.upload_csv_to_s3(csv_dir,
                                                         bucket_name,
                                                         client_obj,
                                                         resource_obj,
                                                         **airflow_context)

            # verify the uploaded files now exist in the bucket, we just
            # uploaded three files, so the number of objects in the bucket
            # now should be three
            bucket_contents_after_upload = len(list(bucket.objects.all()))

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # clean up the s3 bucket
        self.teardown_s3_bucket_res(bucket_name)

        # Assert
        # upload function returns True if it was called
        assert status is True

        # confirm that before uploading the bucket is empty and after uploading
        # there are three files
        assert bucket_contents_before_upload == 0
        assert bucket_contents_after_upload == 3

    def test_upload_directory_check_success_with_csv_present(self,
                                                             airflow_context):
        """returns appropiate status message on detecting valid csv
        files in the csv directory.
        """

        # Arrange

        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # status of the directory check operation and value of the data
        stat = None
        msg = None
        val = None

        # path to fakes news and csv directories the function
        # under test uses
        csv_dir = os.path.join('tempdata', pipeline_name, 'csv')

        # create dummy csv files
        file_path_one = os.path.join(csv_dir, 'stuff1.csv')
        file_path_two = os.path.join(csv_dir, 'stuff2.csv')
        file_path_three = os.path.join(csv_dir, 'stuff3.csv')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            patcher.fs.create_dir(csv_dir)
            patcher.fs.create_file(file_path_one, contents='1,dummy,txt')
            patcher.fs.create_file(file_path_two, contents='2,dummy,rtf')
            patcher.fs.create_file(file_path_three, contents='3,dummy,doc')

        # Act
            # with csv files present, success status message is returned
            stat, msg, val = c.UploadOperations.upload_directory_check(csv_dir)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert "CSV files present" in msg
        assert stat is True
        assert val == ['stuff1.csv', 'stuff2.csv', 'stuff3.csv']

    def test_upload_directory_check_empty_dir_fails(self, airflow_context):
        """returns appropiate status message on detecting empty directory."""

        # Arrange

        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # status of the directory check operation and value of the data
        stat = None
        msg = None
        val = None

        # path to fakes news and csv directories the function
        # under test uses
        csv_dir = os.path.join('tempdata', pipeline_name, 'csv')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            patcher.fs.create_dir(csv_dir)

        # Act
            # with csv files present, success status message is returned
            stat, msg, val = c.UploadOperations.upload_directory_check(csv_dir)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert "Directory is empty" in msg
        assert stat is True
        assert not val

    @pytest.mark.skip
    def test_upload_directory_check_no_csvs_fails(self, airflow_context):
        """returns appropiate status message on detecting no csv files
        in the csv directory.
        """

        # Arrange

        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # status of the directory check operation and value of the data
        stat = None
        msg = None
        val = None

        # path to fakes news and csv directories the function
        # under test uses
        csv_dir = os.path.join('tempdata', pipeline_name, 'csv')

        # create dummy csv files
        file_path_one = os.path.join(csv_dir, 'stuff1.txt')
        file_path_two = os.path.join(csv_dir, 'stuff2.rtf')
        file_path_three = os.path.join(csv_dir, 'stuff3.doc')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            patcher.fs.create_dir(csv_dir)
            patcher.fs.create_file(file_path_one)
            patcher.fs.create_file(file_path_two)
            patcher.fs.create_file(file_path_three)

        # Act
            # with csv files present, success status message is returned
            stat, msg, val = c.UploadOperations.upload_directory_check(csv_dir)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert "Directory has no csv-headline files" in msg
        assert stat is True
        assert val == ['stuff1.txt', 'stuff2.rtf', 'stuff3.doc']

    def test_upload_directory_check_blank_dir_path_fails(self,
                                                         airflow_context):
        """returns appropiate status message on encountering errors
        reading the csv directory.
        """

        # Arrange

        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # status of the directory check operation and value of the data
        stat = None
        msg = None
        val = None

        # path to fakes news and csv directories the function
        # under test uses
        csv_dir = os.path.join('tempdata', pipeline_name, 'csv')

        # create dummy csv files
        file_path_one = os.path.join(csv_dir, 'stuff1.csv')
        file_path_two = os.path.join(csv_dir, 'stuff2.csv')
        file_path_three = os.path.join(csv_dir, 'stuff3.csv')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            patcher.fs.create_dir(csv_dir)
            patcher.fs.create_file(file_path_one, contents='1,dummy,txt')
            patcher.fs.create_file(file_path_two, contents='2,dummy,rtf')
            patcher.fs.create_file(file_path_three, contents='3,dummy,doc')

        # Act
            # with csv files present, success status message is returned
            stat, msg, val = c.UploadOperations.upload_directory_check(csv_dir)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert "CSV files present" in msg
        assert stat is True
        assert val == ['stuff1.csv', 'stuff2.csv', 'stuff3.csv']

    @mock_s3
    def test_upload_csv_to_s3_fails_with_empty_csv_dir(self,
                                                       airflow_context,
                                                       bucket_names,
                                                       home_directory_res):
        """uploading fails if the directory is empty."""

        # Arrange

        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # S3 bucket to upload the file to
        bucket_name = bucket_names[0]

        # create the s3 client and resource objects required
        client_obj, resource_obj = self.setup_s3_bucket_res(bucket_name)

        # path to fake news and csv directories the function under test uses
        csv_dir = os.path.join(home_directory_res,
                               'tempdata',
                               pipeline_name,
                               'csv')

        news_dir = os.path.join(home_directory_res,
                                'tempdata',
                                pipeline_name,
                                'news')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem empty directory to test the method
            patcher.fs.create_dir(csv_dir)
            patcher.fs.create_dir(news_dir)

        # Act
            # function should raise errors on an empty directory
            stat, msg = c.UploadOperations.upload_csv_to_s3(csv_dir,
                                                            bucket_name,
                                                            client_obj,
                                                            resource_obj,
                                                            **airflow_context)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # clean up the s3 bucket
        self.teardown_s3_bucket_res(bucket_name)

        # Assert
        assert "Directory is empty" in msg

    @mock_s3
    def test_upload_csv_to_s3_non_existent_bucket_fails(self,
                                                        airflow_context,
                                                        home_directory_res):
        """uploading fails if the s3 bucket location does not already exist."""

        # Arrange

        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # S3 bucket to upload the file to
        bucket_name = 'non-existent-bucket-name'

        # create the s3 client and resource objects required
        client_obj, resource_obj = self.setup_s3_bucket_res(bucket_name,
                                                            False)

        # path to fake news and csv directories the function under test uses
        csv_dir = os.path.join(home_directory_res,
                               'tempdata',
                               pipeline_name,
                               'csv')

        news_dir = os.path.join(home_directory_res,
                                'tempdata',
                                pipeline_name,
                                'news')

        # create dummy csv files that will be uploaded by the function
        file_path_one = os.path.join(csv_dir, 'stuff1.csv')
        file_path_two = os.path.join(csv_dir, 'stuff2.csv')
        file_path_three = os.path.join(csv_dir, 'stuff3.csv')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and place the dummy csv files
            # in that directory to test the method
            patcher.fs.create_dir(csv_dir)
            patcher.fs.create_dir(news_dir)
            patcher.fs.create_file(file_path_one, contents='1,dummy,txt')
            patcher.fs.create_file(file_path_two, contents='2,dummy,rtf')
            patcher.fs.create_file(file_path_three, contents='3,dumy,doc')

        # Act
            # with no valid bucket existing on the server
            # the function should raise errors
            with pytest.raises(FileNotFoundError) as err:
                c.UploadOperations.upload_csv_to_s3(csv_dir,
                                                    bucket_name,
                                                    client_obj,
                                                    resource_obj,
                                                    **airflow_context)

            actual_message = str(err.value)
            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert "does not exist on the server" in actual_message

    @mock_s3
    def test_upload_csv_to_s3_no_csvs_in_directory_fails(self,
                                                         airflow_context,
                                                         bucket_names,
                                                         home_directory_res):
        """function fails if there are no csv-headline files in the
        directory.
        """

        # Arrange

        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # S3 bucket to upload the file to
        bucket_name = bucket_names[0]

        # create the s3 client and resource objects required
        client_obj, resource_obj = self.setup_s3_bucket_res(bucket_name)

        # path to fake news and csv directories
        csv_dir = os.path.join(home_directory_res,
                               'tempdata',
                               pipeline_name,
                               'csv')

        news_dir = os.path.join(home_directory_res,
                                'tempdata',
                                pipeline_name,
                                'news')

        # create dummy non-csv files
        file_path_one = os.path.join(csv_dir, 'stuff1.txt')
        file_path_two = os.path.join(csv_dir, 'stuff2.rtf')
        file_path_three = os.path.join(csv_dir, 'stuff3.doc')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            patcher.fs.create_dir(csv_dir)
            patcher.fs.create_dir(news_dir)
            patcher.fs.create_file(file_path_one, contents='dummy txt')
            patcher.fs.create_file(file_path_two, contents='dummy rtf')
            patcher.fs.create_file(file_path_three, contents='dummy doc')

        # Act
            # function should raise errors on an no csv files present
            stat, msg = c.UploadOperations.upload_csv_to_s3(csv_dir,
                                                            bucket_name,
                                                            client_obj,
                                                            resource_obj,
                                                            **airflow_context)

            # clean up and remove the fake filesystem
            patcher.tearDown()

        # clean up the s3 bucket
        self.teardown_s3_bucket_res(bucket_name)

        # Assert
        assert "Directory has no csv-headline files" in msg

    def test_upload_csv_to_s3_fails_with_no_bucket_name(self,
                                                        airflow_context,
                                                        bucket_names,
                                                        home_directory_res):
        """function fails if the bucket name is left blank"""

        # Arrange

        # setup a Mock of the boto3 resources and file upload functions
        upload_client = MagicMock(spec=boto3.client('s3'))
        resource_client = MagicMock(spec=boto3.resource('s3'))
        upload_client.upload_file.side_effect = lambda: None
        resource_client.buckets.all.side_effect = lambda: bucket_names

        # get the current pipeline info
        pipeline_name = airflow_context['dag'].dag_id

        # S3 bucket to upload the file to
        bucket_name = None

        # path to fakes news and csv directories the function under test uses
        csv_dir = os.path.join(home_directory_res,
                               'tempdata',
                               pipeline_name,
                               'csv')

        news_dir = os.path.join(home_directory_res,
                                'tempdata',
                                pipeline_name,
                                'news')

        # create dummy csv files
        file_path_one = os.path.join(csv_dir, 'stuff1.csv')
        file_path_two = os.path.join(csv_dir, 'stuff2.csv')
        file_path_three = os.path.join(csv_dir, 'stuff3.csv')

        with Patcher() as patcher:
            # setup pyfakefs - the fake filesystem
            patcher.setUp()

            # create a fake filesystem directory and files to test the method
            patcher.fs.create_dir(csv_dir)
            patcher.fs.create_dir(news_dir)
            patcher.fs.create_file(file_path_one, contents='1,dummy,txt')
            patcher.fs.create_file(file_path_two, contents='2,dummy,rtf')
            patcher.fs.create_file(file_path_three, contents='3,dummy,doc')

        # Act
            # function should raise errors on a null bucket name
            with pytest.raises(ValueError) as err:
                c.UploadOperations.upload_csv_to_s3(csv_dir,
                                                    bucket_name,
                                                    upload_client,
                                                    resource_client,
                                                    **airflow_context)

            actual_message = str(err.value)
            # clean up and remove the fake filesystem
            patcher.tearDown()

        # Assert
        assert "Bucket name cannot be empty" in actual_message
