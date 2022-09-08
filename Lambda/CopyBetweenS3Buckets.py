import json
import urllib.request, urllib.parse, urllib.error
import time
import os.path
import boto3
# from __future__ import print_function

s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    #for record in event['Records']:
    #    bucket = record[0]['s3']['bucket']['name']
    #    key = record[0]['s3']['object']['key']
    #    response = s3.head_object(Bucket=bucket, Key=key)
    #    logger.info('Response: {}'.format(response))
    #    print("Author : " + response['Metadata']['author'])
    #    print("Description : " + response['Metadata']['description'])
        
        
    # print("Received event: " + json.dumps(event, indent=2))
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    copy_source = {'Bucket' : source_bucket, 'Key' : key}
    print(copy_source)
    
    
    #response = s3.get_object(Bucket=source_bucket, Key=key)
    #print("CONTENT TYPE: " + response['ContentType'])
    #return response['ContentType']
    
    
    try:
        waiter  = s3.get_waiter('object_exists')
        waiter.wait(Bucket=source_bucket, Key=key)
        
        # Get the File Extension
        extension = os.path.splitext(key)[1]
        
        # Copy Objects to Destined Buckets based on filetype
        if extension == '.csv':
            s3.copy_object(Bucket='processonglue-csv', Key='CopiedFiles/'+key, CopySource=copy_source)
        if extension == '.json':
            s3.copy_object(Bucket='processonglue-json', Key='CopiedFiles/'+key, CopySource=copy_source)
        if extension == '.xml':
            s3.copy_object(Bucket='processonglue-xml', Key='CopiedFiles/'+key, CopySource=copy_source)
    
    except Exception as e:
        print(e)
        print('Error While Trying to Copy the file')
        raise e
        
