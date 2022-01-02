import boto3
import argparse
from pprint import pprint
import os
from tqdm import tqdm

# run it like this
# python3 s3-cleaner.py -p kloia -b pulumi-demo-bucket-cc7da5b -r us-west-1
parser = argparse.ArgumentParser(description='Process some integers.')
# parser.add_argument('-p', '--profile',  type=str, help='AWS CLI Profile name')
# parser.add_argument('-r', '--region',  type=str, help='AWS CLI Region name')
parser.add_argument('-b', '--bucket',  type=str, help='AWS S3 Bucket name')

args = parser.parse_args()

# print(args.region)
print(args.bucket)
# print(args.profile)

AWS_REGION = 'us-east-1'
AWS_PROFILE = 'localstack'
ENDPOINT_URL = "http://localhost:4566"

boto3.setup_default_session(profile_name=AWS_PROFILE)

loaclstack_client = boto3.client("s3", region_name=AWS_REGION,
                         endpoint_url=ENDPOINT_URL)



# session = boto3.Session(
#     region_name=args.region,
#     profile_name=args.profile
# )

boto3.setup_default_session(profile_name=AWS_PROFILE)
s3 = boto3.client('s3',region_name=AWS_REGION,
                         endpoint_url=ENDPOINT_URL)




class S3Cleaner:
    def __init__(self, s3):
      self.s3 = s3
    
    all_obj=[]

    def get_all_objects(self,bucket_name,prefix=""):
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        all_obj=[]
        for page in pages:
            page_content= page.get('Contents')

            if page_content:
                for obj in page['Contents']:
                    all_obj.append(obj)
            else:
                print(f"{bucket_name} Bucket is empty")
        return all_obj

    def delete_all_objects(self,bucket_name,prefix=""):
        all_obj= self.get_all_objects(bucket_name=bucket_name)
        failed_obj=[]
        if len(all_obj).__eq__(0) : return {"message": "Bucket is empty"}

        pprint(str(len(all_obj)) + " object will be deleted from " + f"'{bucket_name}' bucket")
        answer=input("Y or N ?")
        if answer.__eq__("Y"):
            for obj in tqdm(all_obj):
                response = self.s3.delete_object(Bucket=bucket_name,Key=obj['Key'])
                status = response['ResponseMetadata']['HTTPStatusCode']
                if not status.__eq__(204):
                    print(f"{obj['Key']} file could not deleted")
                    failed_obj.append(obj)
                
            if len(failed_obj).__eq__(0):
                print(f"All objects deleted from {bucket_name} Bucket")
                return {"message": "All objects deleted"}
            else:
                for obj in failed_obj:
                    print(f"Could not delete {obj['Key']}")
                return {"message": "Failed to delete all objects","failed_objects": failed_obj}
                    
        else:
            pass

    def deleteVersioningBucket(self,bucket_name):
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
                    objects.append({"VersionId": v["VersionId"], "Key": v["Key"]})
                self.s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
            except:
                pass


            try:
                objects = []
                delete_markers = version_list["DeleteMarkers"]
                print("Deleting markers")
                for d in tqdm(delete_markers):
                    objects.append({"VersionId": d["VersionId"], "Key": d["Key"]})
                self.s3.delete_objects(Bucket=bucket_name, Delete={"Objects": objects})
            except:
                pass

            is_truncated = version_list.get("IsTruncated")
            key_marker = version_list.get("NextKeyMarker")

        bucket_empty = (
        self.s3.list_object_versions(Bucket=bucket_name, MaxKeys=max_keys).get("Versions")
        == None)
        if bucket_empty:
            print("Successfully deleted all objects/versions in bucket.")
        else:
            print("Not all objects/versions deleted. Please, investigate.")


cleaner = S3Cleaner(s3)

bucket_name = args.bucket
bucket = s3.get_bucket_versioning(Bucket=bucket_name)

if bucket['Status'].__eq__('Enabled'):
    print('Bucket versioning enabled.')
    all_obj= cleaner.get_all_objects(bucket_name=bucket_name)
    pprint(str(len(all_obj)) + " objects/delete markers/versions will be deleted from " + f"'{bucket_name}' bucket")
    answer=input("Y or N ?")
    if answer.__eq__("Y"):
        cleaner.deleteVersioningBucket(bucket_name)
else:
    print('Bucket versioning suspended.')
    all_obj= cleaner.get_all_objects(bucket_name=bucket_name)
    pprint(str(len(all_obj)) + " objects will be deleted from " + f"'{bucket_name}' bucket")
    answer=input("Y or N ?")
    if answer.__eq__("Y"):
        cleaner.delete_all_objects(bucket_name)

