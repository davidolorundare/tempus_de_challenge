import boto3
import logging
import os


log = logging.getLogger(__name__)


class UploadOperations:
    """handles functionality for uploading flattened CSVs in a directory.

    Reads all files in the 'csv' directory and uploads them to a preexisting
    Amazon S3 bucket using the native Amazon Python (boto) library.

    The upload function works ONLY with preexisting buckets and strictly
    assumes that the user has created two buckets in AWS S3 named:
    'tempus-challenge-csv-headlines' and 'tempus-bonus-challenge-csv-headlines'
    """

    # store the current directory of the airflow home folder
    # airflow creates a home environment variable pointing to the location
    HOME_DIRECTORY = str(os.environ['HOME'])

    @classmethod
    def upload_csv_to_s3(cls,
                         csv_directory,
                         bucket_name=None,
                         aws_service_client=None,
                         aws_resource=None,
                         **context):
        """uploads a files, in a given directory, to an Amazon S3 bucket
        location.

        # Arguments:
            :param csv_directory: path to the directory containing all the
                csv headline files.
            :type csv_directory: str
            :param bucket_name: name of an existing s3 bucket
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
                pipeline
            :type context: dict
        """

        log.info("Running upload_news_headline_csv_to_s3 method")

        # instantiate an S3 client object which will perform the uploads
        if not aws_service_client:
            aws_service_client = boto3.client('s3')
        if not aws_resource:
            aws_resource = boto3.resource('s3')
        upload_status = None

        # ensure pre-existence of the bucket
        if not bucket_name:
            raise ValueError("Bucket name cannot be empty")
        if bucket_name not in aws_resource.buckets.all():
            upload_status = False
            raise FileNotFoundError("Bucket {} does not exist on the server\
                ".format(bucket_name))

        # list of all csv files in the directory
        csv_files = []

        # error-checks
        if not os.listdir(csv_directory):
            upload_status = False
            raise FileNotFoundError("Directory is empty")

        if os.listdir(csv_directory):
            csv_files = [file for file in os.listdir(csv_directory)
                         if file.endswith('.csv')]

        # check existence of csv files and the bucket itself before beginning
        # the upload
        if not csv_files:
            upload_status = False
            raise FileNotFoundError("Directory has no csv-headline files")

        # iterate through the files in the directory and upload them to s3
        for file in csv_files:
            file_path = os.path.join(csv_directory, file)
            aws_service_client.upload_file(file_path, bucket_name, file)

        # file upload successful if it reached this point without any errors
        upload_status = True

        # return upload status to calling function
        return upload_status
