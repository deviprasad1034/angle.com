
import os
import json
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import urllib
from urllib import request
from pyspark.sql import SparkSession
from sys import argv
import sys



conf = SparkConf().setAppName("pyspark").setMaster("yarn")
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .getOrCreate()



spark = SparkSession.builder.getOrCreate()




snowpass = os.popen("aws secretsmanager get-secret-value --secret-id snowpassword").read()

import json

data = json.loads(str(snowpass))
secretString = json.loads(data["SecretString"])
password = secretString["snowpassword"]

print("=========snowpassword =========")
print(password)
print("=========snowpassword =========")





snowdf = spark.read.format("snowflake").option("sfURL","https://hgffczj-aj09505.snowflakecomputing.com").option("sfAccount","hgffczj").option("sfUser","sivavasusaia").option("sfPassword",password).option("sfDatabase","zeyodb").option("sfSchema","zeyoschema").option("sfRole","ACCOUNTADMIN").option("sfWarehouse","COMPUTE_WH").option("dbtable","srctab").load()




aggdf = snowdf.groupBy("username").agg(count("site").alias("sitecount"))



aggdf.write.format("parquet").mode("overwrite").save("s3://zeyoauto/dest/sitecount")











