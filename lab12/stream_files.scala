// Databricks notebook source
// MAGIC %md ## Dane
// MAGIC Dane są dostępne na AWS i dostęp zapewnia Databricks `/databricks-datasets/structured-streaming/events/` 

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/structured-streaming/events/

// COMMAND ----------



// COMMAND ----------

// MAGIC %fs head /databricks-datasets/structured-streaming/events/file-0.json

// COMMAND ----------

// MAGIC %md 
// MAGIC * Stwórz osobny folder 'streamDir' i przekopuj 40 plików. możesz użyć dbutils....
// MAGIC * Pozostałe pliki będziesz kopiować jak stream będzie aktywny

// COMMAND ----------

val streamPath = "/data/stream_files"
dbutils.fs.mkdirs(streamPath)


// COMMAND ----------

import java.io.File;

val files= dbutils.fs.ls("/databricks-datasets/structured-streaming/events/").take(40)
for ( i <- 0 to 39){
val path = files(i).path;
  var file = new File(path);
 
   dbutils.fs.cp( path, "/data/stream_files/".concat( file.getName()))
}




// COMMAND ----------

// MAGIC %fs ls "/data/stream_files/"

// COMMAND ----------

// MAGIC %md ## Analiza danych
// MAGIC * Stwórz schemat danych i wyświetl zawartość danych z oginalnego folderu

// COMMAND ----------

import org.apache.spark.sql.types._

val inputPath = "/databricks-datasets/structured-streaming/events/"

val jsonSchema = StructType(Array(
    StructField("time",TimestampType,true),
    StructField("action",StringType,true),

  ))

val staticInputDF = spark.read.schema(jsonSchema).json(inputPath)

display(staticInputDF)

// COMMAND ----------

// MAGIC %md 
// MAGIC Policz ilość akcji "open" i "close" w okienku (window) jedno godzinnym (kompletny folder). 

// COMMAND ----------

import org.apache.spark.sql.functions._

val staticCountsDF =staticInputDF.groupBy(window($"time", "1 hours"),$"action", $"time" ).count().alias("total_count")

staticCountsDF.createOrReplaceTempView("static_counts")

// COMMAND ----------

// MAGIC %md 
// MAGIC Użyj sql i pokaż na wykresie ile było akcji 'open' a ile 'close'.

// COMMAND ----------

// MAGIC %sql select action, sum(count) as total_count from static_counts group by action

// COMMAND ----------

// MAGIC %md
// MAGIC Użyj sql i pokaż ile było akcji w każdym dniu i godzinie przykład ('Jul-26 09:00')

// COMMAND ----------

// MAGIC %sql select * from static_counts

// COMMAND ----------

// MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from static_counts order by time, action

// COMMAND ----------

// MAGIC %md ## Stream Processing 
// MAGIC Teraz użyj streamu.
// MAGIC * Ponieważ będziesz straemować pliki trzeba zasymulować, że jest to normaly stream. Podpowiedź dodaj opcję 'maxFilesPerTrigger'
// MAGIC * Użyj 'streamDir' niekompletne pliki

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDF = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)


val streamingCountsDF = streamingInputDF.withWatermark("time", "1 hour").groupBy($"action", window($"time", "1 hour")).count()


// COMMAND ----------

// MAGIC %md
// MAGIC Sprawdź czy stream działa

// COMMAND ----------

streamingCountsDF.isStreaming

// COMMAND ----------

// MAGIC %md 
// MAGIC * Zredukuj partyce shuffle do 4 
// MAGIC * Teraz ustaw Sink i uruchom stream
// MAGIC * użyj formatu 'memory'
// MAGIC * 'outputMode' 'complete'

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",4)


val query = streamingCountsDF.writeStream.option("checkpointLocation", "/data/checkpoints").outputMode("append").format("json").option("path", streamPath).queryName("counts").start();

// COMMAND ----------

// MAGIC %md 
// MAGIC `query` działa teraz w tle i wczytuje pliki cały czas uaktualnia count. Postęp widać w Dashboard

// COMMAND ----------

Thread.sleep(3000) // lekkie opóźnienie żeby poczekać na wczytanie plików

// COMMAND ----------

// MAGIC %md
// MAGIC * Użyj sql żeby pokazać ilość akcji w danym dniu i godzinie 

// COMMAND ----------

// MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, action

// COMMAND ----------

// MAGIC %md 
// MAGIC * Sumy mogą się nie zgadzać ponieważ wcześniej użyłeś niekompletnych danych.
// MAGIC * Teraz przekopiuj resztę plików z orginalnego folderu do 'streamDir', sprawdź czy widać zmiany 

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from static_counts order by time, action
// MAGIC 
// MAGIC -- użyj zapytania jak wcześniej pokazujący symy z datą i godziną powinny pasować do danych z pierwszego statycznego DF

// COMMAND ----------

// MAGIC %md
// MAGIC * Zatrzymaj stream

// COMMAND ----------

query.stop()
