import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# get data from crawler/ table and create dynamic frame 
datasource = glueContext.create_dynamic_frame.from_catalog(database = "braindroogdb", table_name = "senate_term_16_loksabha_csv", transformation_ctx = "datasource")

# dynamicframe convert to dataframe
df = datasource.toDF()

# collect_set() does not work if output stored in Database
# collection items also not stored in csv files. so, use json as output for collect_set() or collect_list()
# This job will fail only when exporting dynamicframe/dataframe
# export this frame at last to see if Glue Jobs are ACID
# other tasks in this job will not fail, frames df2, df3, df4 will be exported successfully...
df1 = df.groupBy(df.group).agg(collect_set(col("area_id")))

#df.createOrReplaceTempView("tab")
#df1 = spark.sql("SELECT nvl2(email,1,0) + nvl2(twitter,1,0) + nvl2(facebook,1,0) as total from tab")

df2 = df.withColumn("emails", substring_index(col("email"), "@", -1)).withColumn("username", substring_index(col("email"), "@", 1))\
        .drop("id", "name", "sort_name", "twitter", "facebook", "start_date", "end_date", "image", "area_id", "chamber", "term")

# Find Unique Email Handles that are used by Politicians
df3 = df.select(regexp_extract(col("email"), "@+\S+", 0).alias("handles")).drop_duplicates().dropna()
mailcount = df.select(regexp_extract(col("email"), "@+\S+", 0).alias("handles")).dropna()
df4 = mailcount.groupBy(col("handles")).agg(count('*'))

df11 = DynamicFrame.fromDF(df1, glueContext, 'res1')
df22 = DynamicFrame.fromDF(df2, glueContext, 'res2')
df33 = DynamicFrame.fromDF(df3, glueContext, 'res3')
df44 = DynamicFrame.fromDF(df4, glueContext, 'res4')

frames  = [df22, df33, df44, df11]

a = 1
for framex in frames:
    datasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame = framex, catalog_connection = "rdsmysqldb", connection_options = {"dbtable": "term_16_loksabha_" + str(a), "database": "sparkdb"}, transformation_ctx = "datasink4")
    
    #s3_write_path = "s3://gule270822/OutputDatasets/term_16_loksabha_" + str(a)
    #datasink = glueContext.write_dynamic_frame.from_options(frame= framex, connection_type="s3", connection_options={"path": s3_write_path}, format="csv", transformation_ctx="datasink")
    a += 1
    
job.commit()
