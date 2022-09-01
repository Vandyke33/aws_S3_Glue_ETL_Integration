def read_nested_json(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name,explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df
    
def flatten(df):
  read_nested_json_flag = True
  while read_nested_json_flag:
    df = read_nested_json(df);
    read_nested_json_flag = False
    for column_name in df.schema.names:
      if isinstance(df.schema[column_name].dataType, ArrayType):
        read_nested_json_flag = True
      elif isinstance(df.schema[column_name].dataType, StructType):
        read_nested_json_flag = True;
  return df;

data = r"c:/Datasets/file.json"

# Reading JSON file here in Pyspark Session, you can use pandas DataFrame aslo...

df = spark.read.json(data)
df1 = flatten(df)
df1.printSchema()
