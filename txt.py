import logging
import boto3
from botocore.exceptions import ClientError
import os
import json

AWS_REGION = 'us-east-1'
AWS_PROFILE = 'localstack'
ENDPOINT_URL = "http://localhost:4566"





# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

boto3.setup_default_session(profile_name=AWS_PROFILE)

s3_client = boto3.client("s3", region_name=AWS_REGION,
                         endpoint_url=ENDPOINT_URL)


def writeTxtTestFiles():
    for i in range(1,2001):
        with open(file=f"./testFiles/{i}.txt",mode="a") as f:
            print(i,file=f)

def create_bucket(bucket_name):
    """
    Creates a S3 bucket.
    """
    try:
        response = s3_client.create_bucket(
            Bucket=bucket_name)
    except ClientError:
        logger.exception('Could not create S3 bucket locally.')
        raise
    else:
        return response

def upload_file(file_name, bucket, object_name=None):
    """
    Upload a file to a S3 bucket.
    """
    try:
        if object_name is None:
            object_name = os.path.basename(file_name)
        response = s3_client.upload_file(
            file_name, bucket, object_name)
    except ClientError:
        logger.exception('Could not upload file to S3 bucket.')
        raise
    else:
        return response


def uploadTestObjects():
    bucket = 'hands-on-cloud-localstack-bucket-1'
    for i in range(1,2001):
        file_name = f"./testFiles/{i}.txt"
        object_name = f'{i}.txt'
        logger.info(f'Uploading {object_name} to S3 bucket in LocalStack...')
        s3 = upload_file(file_name, bucket, object_name)
        logger.info(f'{object_name} uploaded to S3 bucket successfully.')
        print(s3)

    

def main():
    """
    Main invocation function.
    """
    # bucket_name = "hands-on-cloud-localstack-bucket-1"
    # logger.info('Creating S3 bucket locally using LocalStack...')
    # s3 = create_bucket(bucket_name)
    # bucket = s3_client.get_bucket_versioning(Bucket="hands-on-cloud-localstack-bucket-1")
    # # bucket.enable()
    # print(bucket['Status'])
    uploadTestObjects()


if __name__ == '__main__':
    main()