
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType,  DoubleType, IntegerType


spark = SparkSession.builder.master(
    "local[*]").appName("Lab2_main").getOrCreate()


# Exercise 1

custom_schema = StructType([StructField("imdb_title_id", StringType(), True), StructField("title", StringType(), True), StructField("original_title", StringType(), True), StructField("year", StringType(), True), StructField("date_published", StringType(), True), StructField("genre", StringType(), True), StructField("duration", IntegerType(), True), StructField("country", StringType(), True), StructField("language", StringType(), True), StructField("director", StringType(), True), StructField("writer", StringType(), True), StructField(
    "production_company", StringType(), True), StructField("actors", StringType(), True), StructField("description", StringType(), True), StructField("avg_vote", StringType(), True), StructField("votes", IntegerType(), True), StructField("budget", StringType(), True), StructField("usa_gross_income", StringType(), True), StructField("worlwide_gross_income", StringType(), True), StructField("metascore", DoubleType(), True), StructField("reviews_from_users", DoubleType(), True), StructField("reviews_from_critics", DoubleType(), True)])

df = spark.read.format("csv").option("header", True).schema(
    custom_schema).load("data/movies.csv")
df.show(2)

# Exercise 2


# creating json file

json_path = "movies.json"
data_for_json_df = df.limit(10)
data_for_json_df.write.mode("overwrite").json(json_path)
schema_json = custom_schema  # reusing previously created schema

movies_from_json_df = df = spark.read.schema(schema_json).json(json_path)
movies_from_json_df.show()

# Exercise 4

movies_small_schema = StructType()
movies_small_schema.add(StructField("title", StringType(), True))
movies_small_schema.add(StructField("year", IntegerType(), True))
movies_small_schema.add(StructField("duration", IntegerType(), True))
movies_small_schema.add(StructField("language", StringType(), True))

read_options = spark.read.format("csv").schema(movies_small_schema).option(
    "header", True).option("badRecordsPath", "/bad_records")


movies_small_path = "data/movies_small.csv"

movies_small_df = read_options.option(
    "mode", "permissive").load(movies_small_path)
movies_small_df.show()

# After: all records have been read, null was inserted in corrupted cells

movies_small_df = read_options.option("mode", "dropMalformed").load(
    movies_small_path)
movies_small_df.show()
# # After: 1 record was deleted, record with null has been read, because column is nullable in schema


# Commented out due to raising exception

# movies_small_df = read_options.option("mode", "failFast").load(
# # movies_small_path)
# # movies_small_df.show()
# # After: Read raised exception(really long one) during record parsing"

# Exercise 5

movies_small_df = read_options.option(
    "mode", "permissive").load(movies_small_path)


parquet_path = "movies.parquet"
movies_small_df.write.mode("overwrite").format("parquet").save(parquet_path)
movies_df_parquet = spark.read.format("parquet").schema(
    movies_small_schema).load(parquet_path)
movies_df_parquet.show()
# path is composed of 4 files: _SUCCESS, its crc, parquet file(binary) - part0 and its crc


json_path = "movies.json"
movies_small_df.write.mode("overwrite").format("json").save(json_path)
movies_df_json = spark.read.schema(
    movies_small_schema).json(json_path)
movies_df_json.show()

# path is composed of 4 files: _SUCCESS, its crc, json file - part0  and its crc
