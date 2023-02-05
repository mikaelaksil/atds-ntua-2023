# !apt-get install openjdk-8-jdk-headless -qq > /dev/null
# !wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
# !tar xf spark-3.1.1-bin-hadoop3.2.tgz
#!pip install -q findspark

import os
#os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
#os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop3.2"
#import findspark
#findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("query1-SparkSQL").getOrCreate()
#spark = SparkSession.builder.master("query1-SparkSQL").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark
from pyspark.sql.functions import max
import pandas as pd
from pyspark.sql.functions import month
from pyspark.sql.functions import to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import time

myVar=spark.read.parquet("hdfs://master:9000/user/user/TaxiData")
myVar2= spark.read.option("header", "true").option("inferSchema", "true").format("csv").csv("hdfs://master:9000/user/user/TaxiZoneTable/taxi+_zone_lookup.csv")
#myVar=spark.read.parquet("/content/parquetFilesTotal", engine='pyarrow')
start_time = time.time()


myVar.filter((myVar["tpep_pickup_datetime"] < '2022-03-31 23:59:59') & (myVar["tpep_dropoff_datetime"] > '2022-03-01 00:00:00'))
myVar2.createOrReplaceTempView("taxi_lookup")
myVar.createOrReplaceTempView("taxi_data")
sqlquery = """
WITH edited_lookup AS (
        SELECT *
        FROM taxi_lookup
        WHERE taxi_lookup.Zone == "Battery Park"
)
SELECT max(tip_amount)
FROM taxi_data
INNER JOIN edited_lookup ON taxi_data.DOLocationID == edited_lookup.LocationID
"""
res = spark.sql(sqlquery)
res.show()

print(f"Execution time: {time.time() - start_time}")
