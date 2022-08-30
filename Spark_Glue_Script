import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
###
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# usecasedb
# senate_term_108_csv
# s3://awseverypolitician/input/term-108.csv

db_name="usecasedb"
tb_name="senate_term_108_csv"
data="s3://braindroogetl/inputData/politician_term/term-108.csv"

#get data from crawler/ table and create dynamic frame 
datasource = glueContext.create_dynamic_frame.from_catalog(database=db_name,table_name=tb_name)

#dynamicframe convert to dataframe
df = datasource.toDF()

#data processing
# Find Total number of Parties/Groups
df2 = df.groupBy(df.group).agg(collect_set(col("area_id")))

# Find Total Number of Members in Each Party/Group
df3 = df.groupBy(df.group, df.group_id).agg(count(col("name")).alias("Members"))\
    .orderBy(col("group").asc())

#convert dataframe to gluecontext/dynamicframe 
res1 = DynamicFrame.fromDF(df2, glueContext, 'res1')
res2 = DynamicFrame.fromDF(df3, glueContext, 'res2')

#store data in s3
s3_write_path1 = 's3://awseverypolitician/droog_etl_output/term-108_1.csv'
s3_write_path2 = 's3://awseverypolitician/droog_etl_output/term-108_2.csv'

glueContext.write_dynamic_frame.from_options(frame=res1, connection_type="s3", connection_options={"path": s3_write_path1}, format="json", transformation_ctx="datasink1")

glueContext.write_dynamic_frame.from_options(frame=res2, connection_type="s3", connection_options={"path": s3_write_path2}, format="json", transformation_ctx="datasink2")

job.commit()
