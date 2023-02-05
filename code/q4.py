# apt-get install openjdk-8-jdk-headless -qq > /dev/null
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
import time


myVar=spark.read.parquet("hdfs://master:9000/user/user/TaxiData")

start_time = time.time()

secvar=myVar.withColumn("pickup_hour", hour(myVar["tpep_pickup_datetime"]))
query41=secvar.withColumn("day_of_week", dayofweek(secvar["tpep_pickup_datetime"]))
query42=query41.groupBy("day_of_week", "pickup_hour").agg({"Passenger_count": "sum"})
query43=query42.orderBy(query42["sum(Passenger_count)"].desc())
window = Window.partitionBy("day_of_week").orderBy(query43["sum(Passenger_count)"].desc())
query44 = query43.withColumn("rank", rank().over(window))
query45 = query44.filter(query44["rank"] <= 3)
query46=query45.select("day_of_week", "pickup_hour", "sum(Passenger_count)")
query4=query45.orderBy('day_of_week')
query4.show()
print(f"Execution time: {time.time() - start_time}")
