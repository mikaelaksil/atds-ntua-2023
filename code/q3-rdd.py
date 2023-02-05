
from pyspark.sql.types import FloatType
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, floor, unix_timestamp
from pyspark.sql.functions import max
import pandas as pd
from pyspark.sql.functions import month
from pyspark.sql.functions import to_date
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import hour, dayofweek, rank, year
from pyspark.sql.functions import dayofmonth, month, round, rank
from pyspark.sql.functions import date_trunc, avg, from_unixtime
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import floor
import time
import datetime



def seconds_to_date(seconds):
    return datetime.datetime.fromtimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S')


spark = SparkSession.builder.appName("query3-RDD").getOrCreate()
df = spark.read.parquet("hdfs://master:9000/user/user/TaxiData")


start_time = time.time()

df = df.withColumn("Trip_distance", df["Trip_distance"].cast(FloatType()))

df = df.withColumn("Total_amount", df["Total_amount"].cast(FloatType()))

filtered_df = df.filter(df["PULocationID"] != df["DOLocationID"])

filtered_df = df.filter(year(filtered_df.tpep_pickup_datetime)>2021)


filtered_df = filtered_df.withColumn("pickup_timestamp", unix_timestamp(filtered_df["tpep_pickup_datetime"], "yyyy-MM-dd HH:mm:ss"))
filtered_df = filtered_df.withColumn("dropoff_timestamp", unix_timestamp(filtered_df["tpep_dropoff_datetime"], "yyyy-MM-dd HH:mm:ss"))

filtered_df = filtered_df.withColumn("15-day_interval", floor(filtered_df["pickup_timestamp"] / (60 * 60 * 24 * 15)))
rdd = filtered_df.rdd.map(lambda x: (x["15-day_interval"], (x["Trip_distance"], x["Total_amount"])))
rdd = rdd.aggregateByKey((0, 0, 0),
                         lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + 1),
                         lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2]))
rdd = rdd.mapValues(lambda x: (x[0]/x[2], x[1]/x[2]))


for (key,value) in rdd.collect():
        interval = seconds_to_date(key * 60 * 60 *24 * 15)
        print("15-day interval:", interval, "Avg Distance:", value[0], "Avg Cost:", value[1])

print(f"Execution time: {time.time() - start_time}")
