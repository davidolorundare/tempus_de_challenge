"""Tempus challenge  - Operations and Functions: Upload Tasks

Describes the code definitions used in the Airflow task of uploading
CSV files to an Amazon S3 bucket in the DAG pipelines.
"""

import boto3

import logging
import os

import challenge as c

# ensures that function outputs and any errors encountered
# are logged to the Airflow console
log = logging.getLogger(__name__)


class UploadOperations:
    """Handles functionality for uploading flattened CSVs in a directory.

    Reads all files in the 'csv' directory and uploads them to a preexisting
    Amazon S3 bucket using the native Amazon Python (boto) library.

    The upload function works ONLY with preexisting buckets and strictly
    assumes that the user has created two buckets in AWS S3 named:
    'tempus-challenge-csv-headlines' and 'tempus-bonus-challenge-csv-headlines'
    """

    # store the current directory of the airflow home folder
    # airflow creates a home environment variable pointing to the location.
    HOME_DIRECTORY = str(os.environ['HOME'])

    @classmethod
    def upload_directory_check(cls, csv_dir):
        """performs file checks in a given csv directory.

        # Arguments:
            :param csv_dir: path to the directory containing
                all the csv headline files.
            :type csv_dir: str

        # Raises:
            ValueError: if the csv directory path argument is left blank.
        """

        log.info("Running upload_directory_check method")

        status = None
        message = None

        # list of all csv files in the directory
        csv_files = []

        if not csv_dir:
            raise ValueError("CSV directory path cannot be left blank")

        # check existence of csv files in the directory
        if not os.listdir(csv_dir):
            status = True
            message = "Directory is empty"
            return status, message, csv_files

        if os.listdir(csv_dir):
            csv_files = [file for file in os.listdir(csv_dir)
                         if file.endswith('.csv')]

        # a directory with non-csv files is valid
        if not csv_files:
            status = True
            message = "Directory has no csv-headline files"
            return status, message, csv_files
        else:
            # csv files exist
            status = True
            message = "CSV files present"
            return status, message, csv_files

    @classmethod
    def upload_csv_to_s3(cls,
                         csv_directory=None,
                         bucket_name=None,
                         aws_service_client=None,
                         aws_resource=None,
                         **context):
        """Uploads files, in a given directory, to an Amazon S3 bucket
        location.

        It is a valid state for the csv directory to be empty -
        as this implies no news articles was found during retrieval
        from the newsapi via the upstream tasks. This function does
        nothing if no csv files are in the directory, rather it logs
        this absence of such files and returns a True status to indicate
        that the next task in the pipeline should be executed.

        # Arguments:
            :param csv_directory: path to the directory containing all the
                csv headline files.
            :type csv_directory: str
            :param bucket_name: name of an existing s3 bucket.
            :type bucket_name: str
            :param aws_service_client: reference to an s3 service client object
                instance that should be used. If left blank, the function
                creates a new one.
            :type aws_service_client: object
            :param aws_resource: reference to an s3 resource service
                object instance that should be used. If left blank, the
                function creates a new one.
            :type aws_resource: object
            :param context: airflow context object referencing the current
                pipeline.
            :type context: dict

        # Raises:
            ValueError: if the S3 bucket_name argument is left blank.
            FileNotFoundError: if the bucket has not been already created
                by the user in their Amazon AWS account.
        """

        log.info("Running upload_csv_to_s3 method")

        # get information about the current pipeline
        pipeline_name = context['dag'].dag_id
        pipeline_info = c.NewsInfoDTO(pipeline_name)
        pipeline_csv_dir = pipeline_info.csv_directory

        # inspect the pipeline's csv directory contents
        return_status, msg, data = cls.upload_directory_check(pipeline_csv_dir)
        status = None
        files = None

        if return_status and msg == "Directory is empty":
            status = return_status
            log.info("The csv directory is empty")
            return status, msg
        elif return_status and msg == "Directory has no csv-headline files":
            status = return_status
            log.info("There are no csv-type files in the directory")
            return status, msg
        elif return_status and msg == "CSV files present":
            log.info("CSV files found")
            files = data
        else:
            log.info("Error in reading directory")
            status = return_status
            return status, msg

        # There are csv files to be uploaded. Check pre-existence
        # of a VALID S3 bucket.
        if not bucket_name:
            bucket_name = pipeline_info.s3_bucket_name

        # instantiate an S3 objects which will perform the uploads
        if not aws_service_client:
            aws_service_client = boto3.client('s3')
        if not aws_resource:
            aws_resource = boto3.resource('s3')

        buckets = [bucket.name for bucket in aws_resource.buckets.all()]
        if bucket_name not in buckets:
            status = False
            raise FileNotFoundError("Bucket {} does not exist on the server\
                ".format(bucket_name))

        # iterate through the files in the directory and upload them to s3
        for file in files:
            file_path = os.path.join(pipeline_csv_dir, file)
            aws_service_client.upload_file(file_path, bucket_name, file)

        # file upload successful if it reached this point without any errors
        status = True
        status_msg = "upload successful"

        # return status to calling function
        return status, status_msg
