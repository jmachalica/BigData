// Databricks notebook source
// MAGIC %md
// MAGIC ##1 Kod dodany do jar

// COMMAND ----------

import org.apache.log4j.Logger
lazy val logger = Logger.getLogger(getClass.getName)
logger.info("Info")
logger.warn("Warning")

// COMMAND ----------

// MAGIC %md
// MAGIC ##2

// COMMAND ----------

// MAGIC %md
// MAGIC Dane o dystrybucji -> Executors

// COMMAND ----------

// MAGIC %md
// MAGIC ##3 
// MAGIC 
// MAGIC Kontrolowanie pamieci driver/executor  - spark.driver.memory, spark.executor.memory 

// COMMAND ----------


import org.apache.spark.sql.functions._

val movies_path = "dbfs:/FileStore/tables/Files/movies.csv"

val df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(movies_path).repartition(1)

val new_df = df.withColumn("genre", expr("explode(array_repeat(10,int(100)))" )).repartition(1)


// COMMAND ----------

import org.apache.spark.sql.functions._

df.join(
  broadcast(new_df),
  df("imdb_title_id") <=> new_df("imdb_title_id")
).collect()

// COMMAND ----------

// MAGIC %md
// MAGIC ##4

// COMMAND ----------


val movies_path = "dbfs:/FileStore/tables/Files/movies.csv"
val db_name= "example"
spark.sql(s"CREATE DATABASE  IF NOT EXISTS $db_name")

val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(movies_path)
df.write.mode("overwrite").saveAsTable(s"$db_name.movies")


// COMMAND ----------

val n_buckets= 10
val column= "country"
val bucket_path="/data/bucket"
df.write.format("parquet").mode("overwrite").bucketBy(n_buckets,column).saveAsTable("bucketed")

// COMMAND ----------

// MAGIC %fs ls  /user/hive/warehouse/bucketed

// COMMAND ----------

df.write.format("parquet").mode("overwrite").partitionBy(column).saveAsTable("partitioned")

// COMMAND ----------

// MAGIC %fs ls  /user/hive/warehouse/partitioned

// COMMAND ----------

// MAGIC %md
// MAGIC ##5

// COMMAND ----------

// MAGIC %sql
// MAGIC ANALYZE TABLE bucketed COMPUTE STATISTICS FOR ALL COLUMNS;

// COMMAND ----------

// MAGIC %sql
// MAGIC DESC EXTENDED bucketed;
