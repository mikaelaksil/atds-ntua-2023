# !apt-get install openjdk-8-jdk-headless -qq > /dev/null
# !wget -q http://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
# !tar xf spark-3.1.1-bin-hadoop3.2.tgz
# !pip install -q findspark

#import os
#os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
#os.environ["SPARK_HOME"] = "/content/spark-3.1.1-bin-hadoop3.2"
#import findspark
#findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("query3-SparkSQL").getOrCreate()
#spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark
from pyspark.sql.functions import max
import pandas as pd
from pyspark.sql.functions import month
from pyspark.sql.functions import to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import hour, dayofweek, rank
from pyspark.sql.functions import dayofmonth, month, round, rank
from pyspark.sql.functions import date_trunc, avg
from pyspark.sql.types import TimestampType
import time


myVar=spark.read.parquet("hdfs://master:9000/user/user/TaxiData")
start_time = time.time()


query = """
SELECT
  date_trunc('day', tpep_pickup_datetime) as 15day_interval,
  avg(Trip_distance) as avg_distance,
  avg(Total_amount) as avg_cost
FROM routes
WHERE PULocationID != DOLocationID and year(tpep_pickup_datetime)>2021
GROUP BY 15day_interval
"""

query3=spark.read.parquet("hdfs://master:9000/user/user/TaxiData")
#query3 = spark.read.parquet("/content/parquetFilesTotal",engine='pyarrow')
query3.createOrReplaceTempView("routes")
query3 = spark.sql(query)
query3.show()
