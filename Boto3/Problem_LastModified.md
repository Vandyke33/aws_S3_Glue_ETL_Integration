# Problem Statement − Use boto3 library in Python to get a list of files from S3, those are modified after a given date timestamp.

### Example − List out test.zip from Bucket_1/testfolder of S3 if it is modified after 2021-01-21 13:19:56.986445+00:00.

### Approach/Algorithm to solve this problem
Step 1 − Import boto3 and botocore exceptions to handle exceptions.

Step 2 − s3_path and last_modified_timestamp are the two parameters in function list_all_objects_based_on_last_modified. 
         "last_modified_timestamp" should be in the format “2021-01-22 13:19:56.986445+00:00”. By default, boto3 understands the UTC timezone 
         irrespective of geographical location.

Step 3 − Validate the s3_path is passed in AWS format as s3://bucket_name/key.

Step 4 − Create an AWS session using boto3 library.

Step 5 − Create an AWS resource for S3.

Step 6 − Now list out all the objects of the given prefix using the function list_objects and handle the exceptions, if any.

Step 7 − The result of the above function is a dictionary and it contains all the file-level information in a key named as ‘Contents’. Now extract the bucket-level details in an object.

Step 8 − Now, object is also a dictionary having all the details of a file. Now, fetch LastModified detail of each file and compare with the given date timestamp.

Step 9 − If LastModified is greater than the given timestamp, save the complete file name, else ignore it.

Step 10 − Return the list of files those are modified after the given date timestamp.
