# List Files Based on Last Modified Date

import boto3
from botocore.exceptions import ClientError

def list_all_objects_based_on_last_modified(s3_files_path,
last_modified_timestamp):
   if 's3://' not in s3_files_path:
      raise Exception('Given path is not a valid s3 path.')
   session = boto3.session.Session()
   s3_resource = session.resource('s3')
   bucket_token = s3_files_path.split('/')
   bucket = bucket_token[2]
   folder_path = bucket_token[3:]
   prefix = ""
   for path in folder_path:
      prefix = prefix + path + '/'
   try:
      result = s3_resource.meta.client.list_objects(Bucket=bucket, Prefix=prefix)
   except ClientError as e:
      raise Exception( "boto3 client error in list_all_objects_based_on_last_modified function: " + e.__str__())
   except Exception as e:
      raise Exception( "Unexpected error in list_all_objects_based_on_last_modified
function of s3 helper: " + e.__str__())
   filtered_file_names = []
   for obj in result['Contents']:
      if str(obj["LastModified"]) >= str(last_modified_timestamp):
         full_s3_file = "s3://" + bucket + "/" + obj["Key"]
         filtered_file_names.append(full_s3_file)
      return filtered_file_names

#give a timestamp to fetch test.zip
print(list_all_objects_based_on_last_modified("s3://Bucket_1/testfolder" , "2021-01-21 13:19:56.986445+00:00"))
#give a timestamp no file is modified after that
print(list_all_objects_based_on_last_modified("s3://Bucket_1/testfolder" , "2021-01-21 13:19:56.986445+00:00"))
