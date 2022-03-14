
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.master(
    "local[*]").appName("Lab2_dates").getOrCreate()


# Exercise 3

kolumny = ["timestamp", "unix", "Date"]
dane = [("2015-03-22T14:13:34", 1646641525847, "May, 2021"), ("2015-03-22T15:03:18",
                                                              1646641557555, "Mar, 2021"), ("2015-03-22T14:38:39", 1646641578622, "Jan, 2021")]

df = spark.createDataFrame(dane).toDF(*kolumny).withColumn(
    "current_date", current_date()).withColumn("current_timestamp", current_timestamp())
df.printSchema()


nowyunix = df.select("timestamp", unix_timestamp(
    "timestamp", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp"))


zmianaFormatu = df.withColumnRenamed("timestamp", "xxxx").select(
    "*", unix_timestamp("xxxx", "yyyy-MM-dd HH:mm:ss"))


tempE = df.withColumnRenamed("timestamp", "xxxx").select("*", unix_timestamp("xxxx", "yyyy-MM-dd'T'HH:mm:ss").cast(
    "timestamp")).withColumnRenamed("CAST(unix_timestamp(xxxx, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "xxxcast")


# year month dayofyear

year_month_day_df = df.select(
    "*", year("timestamp"), month("timestamp"), dayofyear("timestamp"))
year_month_day_df.show()
year_month_day_df.printSchema()

# date_format
format_df = df.select("*", date_format("timestamp", "MM/dd/yyyy/H"))
format_df.show()


# to_unix_timestamp()
unix_timestamp_df = df.select(
    "*", unix_timestamp("current_date", "yyyy-MM-dd").alias("current_unix_timestamp"))
unix_timestamp_df.show()


# from_unixtime()
from_unix = unix_timestamp_df.select(
    "*",   from_unixtime("current_unix_timestamp").alias("from_current_unix"))

from_unix.show()


# to_date()
date_df = df.select("*", to_date("current_timestamp", "yyyy-MM-dd"))
date_df.show()
date_df.printSchema()


# to_timestamp
to_timestamp_df = df.select("*", to_timestamp("current_date"))
to_timestamp_df.show()

# from_utc_timestamp()

from_utc_df = df.select(
    "*", from_utc_timestamp("current_timestamp", "+01:00").alias("from_utc"))
from_utc_df.show()

# to_utc_timestamp()

to_utc_df = from_utc_df.select("*", to_utc_timestamp("from_utc", "+01:00"))
to_utc_df.show()
