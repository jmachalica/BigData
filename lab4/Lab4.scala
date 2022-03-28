// Databricks notebook source
// MAGIC %md 
// MAGIC Wykorzystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()

display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

import org.apache.spark.sql.functions._
val table_names= tabela.select(col("TABLE_NAME")).filter('TABLE_SCHEMA === "SalesLT").as[String]
          .collect.toList
table_names

// COMMAND ----------

   val table2= spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.Customer").load()

// COMMAND ----------

val write_format = "delta"
val save_path = "dbfs:/tmp/delta/SalesLT"

var tables:Map[String,org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]] = Map()

for (name <- table_names){
  println(s"Saving $name")
   val table= spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query",s"SELECT * FROM SalesLT.$name").load()
  tables = tables + (name -> table)
  table.write
  .format(write_format)
  .mode("overwrite")
  .save(save_path +s"/$name" )
}

// COMMAND ----------

tables

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------


import org.apache.spark.sql._

def countColNulls(columns:Array[String]):Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}


for ((name, table) <- tables) {
    println(s"Table: $name")
    table.select(countColNulls(table.columns):_*).show()
}


// COMMAND ----------

val tables_null_dropped=tables.map(   {case (name,table) => (name, table.na.drop()) } )

// COMMAND ----------

val example_table=tables_null_dropped("ProductModel")
display(example_table.select(countColNulls(example_table.columns):_*) )  

// COMMAND ----------

// MAGIC %md 
// MAGIC Wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]

// COMMAND ----------

val df=tables("SalesOrderHeader")
display(df)

// COMMAND ----------

import org.apache.spark.sql.functions._
df.select(avg("Freight"), avg("TaxAmt") ).show()

// COMMAND ----------

df.select(variance("Freight"), variance("TaxAmt") ).show()

// COMMAND ----------

df.select(kurtosis("Freight"), kurtosis("TaxAmt") ).show()

// COMMAND ----------

Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
  Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

val df=tables("Product" )
display(df)

// COMMAND ----------

val grouped=df.groupBy("ProductModelId","Color","ProductCategoryID")

// COMMAND ----------

grouped.agg("StandardCost" -> "mean" , "Weight" -> "approx_count_distinct", "ListPrice" -> "max" ).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Stwórz 3 funkcje UDF do wybranego zestawu danych,
// MAGIC 
// MAGIC a. Dwie funkcje działające na liczbach, int, double
// MAGIC 
// MAGIC b. Jedna funkcja na string

// COMMAND ----------

display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.udf

val photo_format = udf( ( value: String )=> { 
  value.split('.').last
} )

df.select(col("ThumbnailPhotoFileName"),photo_format(col("ThumbnailPhotoFileName"))  ).show()


// COMMAND ----------


val cost_weight = udf( ( cost: Double,weight:Double )=> { 
   cost/weight
} )

df.select(col("StandardCost"),col("Weight") ,cost_weight(col("StandardCost"),col("Weight")  ) ).show()


// COMMAND ----------

val price_log = udf( ( price: Double )=> { 
   scala.math.log(price)
} )

df.select(col("ListPrice"), price_log(col("ListPrice") ) ).show()


// COMMAND ----------

val file_location = "dbfs:/FileStore/tables/brzydki.json"
val file_type = "json"
val infer_schema = "true"

val df = spark.read.format(file_type) 
  .option("multiline","true")
  .load(file_location)

display(df)

// COMMAND ----------

df.schema.fields

// COMMAND ----------

// import org.apache.spark.sql.types.{StructType,ArrayType}

// def convertJSON_df(df: DataFrame): DataFrame={
//   df
// }
// TODO


