# Create a Lambda Function with Trigger as s3 path 
# for all create file events in given s3 path

import json
import boto3

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = event['Records'][0]['s3']['object']['key']
    
    glue_client = boto3.client('glue')

    response = glue_client.start_job_run(JobName = 'S3_transform_S3', Arguments={'--buck': bucket_name, '--file' : s3_file_name})
    

# GuleJob Script to be run when event is trigger by Lambda Function
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
args = getResolvedOptions(sys.argv, ["buck","file"])

filename=args['file']
bucket=args['buck']
inputpath="s3a://{}/{}".format(bucket,filename)
outputpath="s3a://awseverypolitician/droog_etl_output/lambdajob"
print("inputpath:", inputpath )

#sc = SparkContext()
#glueContext = GlueContext(sc)
#spark = glueContext.spark_session
#job = Job(glueContext)
#job.init(args["buck","file"], args)

df=spark.read.csv(inputpath, header=True, inferSchema=True, sep=',')


#let eg data in s3 as csv read data from s3 and process
#data processing

df2 = df.groupBy(df.group).agg(collect_set(col("area_id")))

df3 = df.groupBy(df.group, df.group_id).agg(count(col("name")).alias("Members")).orderBy(col("group").asc())

#store cleaned data in s3
df2.coalesce(1).write.csv(outputpath+'1', header=True, sep='::')
df3.coalesce(1).write.csv(outputpath+'2', header=True, sep='::')

#job.commit()
