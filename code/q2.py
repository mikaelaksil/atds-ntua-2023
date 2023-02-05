# !apt-get install openjdk-8-jdk-headless -qq > /dev/null
# !wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
# !tar xf spark-3.1.1-bin-hadoop3.2.tgz
# !pip install -q findspark

import os
#os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
#os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop3.2"
#import findspark
#findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("query2-SparkSQL").getOrCreate()
#spark = SparkSession.builder.master("local[*]").getOrCreate()
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

start_time = time.time()
#myVar=spark.read.parquet("/content/parquetFilesTotal", engine='pyarrow')
second=myVar.filter(myVar['tolls_amount']>0)
third=second.withColumn('month',month(second['tpep_pickup_datetime']))
fourth = third.groupBy("month", "PULocationID", "DOLocationID").agg({"Tolls_amount": "max"})
fifth=fourth.orderBy(fourth["max(Tolls_amount)"].desc())
window = Window.partitionBy("month").orderBy(fifth["max(Tolls_amount)"].desc())
sixth = fifth.withColumn("rank", row_number().over(window))
query2 = sixth.filter(sixth["rank"] == 1)

# Select desired columns and return final DataFrame
query2.select("month", "PULocationID", "DOLocationID", "max(Tolls_amount)")
query2.filter(query2['month']<7).orderBy('month').show()

print(f"Execution time: {time.time() - start_time}")
