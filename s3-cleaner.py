import boto3
import argparse
from pprint import pprint
import os
from tqdm import tqdm
import itertools
from colorama import Fore, Style

# Partition list


def partition(l, size):
    for i in range(0, len(l), size):
        yield list(itertools.islice(l, i, i + size))


# Yes or no dicts
yes = {'yes', 'y', 'ye'}
no = {'no', 'n'}

# run it like this
# python3 s3-cleaner.py -p kloia -b pulumi-demo-bucket-cc7da5b -r us-west-1
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-p', '--profile',  type=str, help='AWS CLI Profile name')
parser.add_argument('-r', '--region',  type=str, help='AWS CLI Region name')
parser.add_argument('-b', '--bucket',  type=str, help='AWS S3 Bucket name')

args = parser.parse_args()

session = boto3.Session(
    region_name=args.region,
    profile_name=args.profile
)
s3 = session.client('s3')


class S3Cleaner:
    def __init__(self, s3):
        self.s3 = s3
        self.all_obj = []

    # Get all objects in the bucket
    def get_all_objects(self, bucket_name, prefix=""):
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        self.all_obj = []
        for page in pages:
            page_content = page.get('Contents')
            if page_content:
                for obj in page['Contents']:
                    self.all_obj.append(obj)
            else:
                pass
        return self.all_obj

    # Delete all object in the not versioned bucket
    def delete_all_objects(self, bucket_name, prefix=""):
        all_obj = self.get_all_objects(bucket_name=bucket_name)
        failed_obj = []

        # If bucket is empty return
        if len(all_obj) == 0:
            return {"message": "Bucket is empty"}

        keys = [{'Key': a_obj.get('Key')}
                for a_obj in all_obj
                ]

        # Delete objects from s3 batch operation
        keys_chunks = list(partition(keys, 500))
        for chunk in tqdm(keys_chunks):
            objects_delete_list = {
                'Objects': chunk
            }
            response = self.s3.delete_objects(
                Bucket=bucket_name, Delete=objects_delete_list)
            status = response['ResponseMetadata']['HTTPStatusCode']
            if 200 <= status < 300:
                pass  # success
            else:
                failed_obj.extend(chunk)

        if len(failed_obj) == 0:
            print(Fore.GREEN +
                  f"Successfully deleted all objects from {bucket_name} Bucket")
            return {"message": "All objects deleted"}
        else:
            for obj in failed_obj:
                print(Fore.RED + f"Could not delete {obj['Key']}")
            return {"message": "Failed to delete all objects", "failed_objects": failed_obj}

    # Delete all objects, versions and delete markers in the bucket
    def delete_versioning_bucket(self, bucket_name):
        max_keys = 1000
        is_truncated = True
        key_marker = None

        while is_truncated == True:

            try:
                if not key_marker:
                    version_list = self.s3.list_object_versions(
                        Bucket=bucket_name, MaxKeys=max_keys
                    )
                else:
                    version_list = self.s3.list_object_versions(
                        Bucket=bucket_name, MaxKeys=max_keys, KeyMarker=key_marker
                    )
            except Exception as e:
                exit(str(e))

            try:
                objects = []
                versions = version_list["Versions"]
                print("Deleting versions")
                for v in tqdm(versions):
                    objects.append(
                        {"VersionId": v["VersionId"], "Key": v["Key"]})
                self.s3.delete_objects(Bucket=bucket_name, Delete={
                                       "Objects": objects})
            except:
                pass

            try:
                objects = []
                delete_markers = version_list["DeleteMarkers"]
                print("Deleting markers")
                for d in tqdm(delete_markers):
                    objects.append(
                        {"VersionId": d["VersionId"], "Key": d["Key"]})
                a = {"Objects": objects}
                self.s3.delete_objects(Bucket=bucket_name, Delete={
                                       "Objects": objects})
            except:
                pass

            is_truncated = version_list.get("IsTruncated")
            key_marker = version_list.get("NextKeyMarker")

        bucket_empty = (
            self.s3.list_object_versions(
                Bucket=bucket_name, MaxKeys=max_keys).get("Versions")
            == None)
        if bucket_empty:
            print(Fore.GREEN + "Successfully deleted all objects/versions in bucket.")
        else:
            print(Fore.RED + "Not all objects/versions deleted. Please, investigate.")


cleaner = S3Cleaner(s3)


bucket_name = args.bucket

# Get bucket version is enabled or suspended
try:
    bucket_versioning_response = s3.get_bucket_versioning(Bucket=bucket_name)
except Exception as e:
    exit(str(e))

is_bucket_versioning_enabled = bucket_versioning_response.get(
    'Status', False) == 'Enabled'


if is_bucket_versioning_enabled:

    print(Fore.WHITE + "Bucket Name: " + Fore.BLUE + f"{bucket_name}")
    print(Fore.YELLOW + 'Bucket versioning enabled.')

    all_obj = cleaner.get_all_objects(bucket_name=bucket_name)
    print(Fore.WHITE + "Object count: " + Fore.BLUE + f"{len(all_obj)}")
    print(Fore.WHITE + "All objects/delete markers/versions will be deleted from " +
          Fore.GREEN + f"'{bucket_name}'" + Fore.WHITE, "bucket")
    answer = input("yes or no? ")

    if answer in yes:
        cleaner.delete_versioning_bucket(bucket_name)
else:
    print(Fore.WHITE + "Bucket Name: " + Fore.BLUE + f"{bucket_name}")
    print(Fore.YELLOW + 'Bucket versioning not available or suspended.')

    all_obj = cleaner.get_all_objects(bucket_name=bucket_name)
    print(Fore.WHITE + "Object count: " + Fore.BLUE + f"{len(all_obj)}")
    if (len(all_obj) == 0):
        print(Fore.WHITE+'Bucket is empty')
        exit()

    print(Fore.WHITE + "All objects will be deleted from " +
          Fore.GREEN + f"'{bucket_name}'" + Fore.WHITE, "bucket")
    answer = input("yes or no? ")

    if answer in yes:
        cleaner.delete_all_objects(bucket_name)
