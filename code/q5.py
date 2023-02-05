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
spark = SparkSession.builder.appName("query5-SparkSQL").getOrCreate()
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
import time


myVar=spark.read.parquet("hdfs://master:9000/user/user/TaxiData")

start_time = time.time()

secvar = myVar.withColumn("month", month(myVar["tpep_pickup_datetime"]))
quer31=secvar.withColumn("day_of_month", dayofmonth(secvar["tpep_pickup_datetime"]))
quer32 = quer31.withColumn("tip_percentage", round((quer31["fare_amount"]-quer31["tip_amount"])/quer31["Fare_amount"]*100, 2))
quer33=quer32.groupBy("day_of_month", "month").agg({"tip_percentage": "avg"})
quer34 = quer33.orderBy(quer33["avg(tip_percentage)"].desc())
window = Window.partitionBy("month").orderBy(quer34["avg(tip_percentage)"].desc())
quer35 = quer34.withColumn("rank", rank().over(window))
query5 = quer35.filter(quer35["rank"] <= 5)

# Select desired columns and return final DataFrame
query5.select("month", "day_of_month", "avg(tip_percentage)").orderBy('month')
query5.filter(query5['month']<7).orderBy('month').show()


print(f"Execution time: {time.time() - start_time}")
