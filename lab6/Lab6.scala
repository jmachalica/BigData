// Databricks notebook source
// MAGIC %md
// MAGIC ## Ex. 1

// COMMAND ----------

val transactions = Seq(( 1, "2011-01-01", 500),
( 1, "2011-01-15", 50),
( 1, "2011-01-22", 250),
( 1, "2011-01-24", 75),
( 1, "2011-01-26", 125),
( 1, "2011-01-28", 175),
( 2, "2011-01-01", 500),
( 2, "2011-01-15", 50),
( 2, "2011-01-22", 25),
( 2, "2011-01-23", 125),
( 2, "2011-01-26", 200),
( 2, "2011-01-29", 250),
( 3, "2011-01-01", 500),
( 3, "2011-01-15", 50 ),
( 3, "2011-01-22", 5000),
( 3, "2011-01-25", 550),
( 3, "2011-01-27", 95 ),
( 3, "2011-01-30", 2500)).toDF("AccountId", "TranDate", "TranAmt")


val logical = Seq((1,"George", 800),
(2,"Sam", 950),
(3,"Diane", 1100),
(4,"Nicholas", 1250),
(5,"Samuel", 1250),
(6,"Patricia", 1300),
(7,"Brian", 1500),
(8,"Thomas", 1600),
(9,"Fran", 2450),
(10,"Debbie", 2850),
(11,"Mark", 2975),
(12,"James", 3000),
(13,"Cynthia", 3000),
(14,"Christopher", 5000)).toDF("RowID" ,"FName" , "Salary" )


// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


val windowSpec  = Window.partitionBy(col("AccountId")).orderBy("TranDate")
val run_total=sum("TranAmt").over(windowSpec)

transactions.select(col("AccountId"), col("TranDate"), col("TranAmt") ,run_total.alias("RunTotalAmt") ).show()


// COMMAND ----------


val windowSpec  = Window.partitionBy(col("AccountId")).orderBy("TranDate")

val tr_avg=avg("TranAmt").over(windowSpec)
val tr_count=count("*").over(windowSpec)
val tr_min=min("TranAmt").over(windowSpec)
val tr_max=max("TranAmt").over(windowSpec)
val tr_sum=sum("TranAmt").over(windowSpec)

val df=transactions.select(col("AccountId"), col("TranDate"), col("TranAmt") ,tr_avg.alias("RunAvg") ,tr_count.alias("RunTranQty") ,tr_min.alias("RunSmallAmt") , tr_max.alias("RunLargeAmt") , tr_sum.alias("RunTotalAmt") )
display(df)

// COMMAND ----------


val windowSpecBase=Window.partitionBy(col("AccountId")).orderBy("TranDate")
val windowSpec  = windowSpecBase.rowsBetween(-2, Window.currentRow) 

val tr_avg=avg("TranAmt").over(windowSpec)
val tr_count=count("*").over(windowSpec)
val tr_min=min("TranAmt").over(windowSpec)
val tr_max=max("TranAmt").over(windowSpec)
val tr_sum=sum("TranAmt").over(windowSpec)
val row_nr= row_number().over(windowSpecBase)

val df=transactions.select(col("AccountId"), col("TranDate"), col("TranAmt") ,tr_avg.alias("SlideAvg") ,tr_count.alias("SlideQty") ,tr_min.alias("SlideMin") , tr_max.alias("SlideMax") , tr_sum.alias("SlideTotal") , row_nr. alias("RN") ).orderBy("AccountId", "TranDate", "RN")
display(df)

// COMMAND ----------


val windowSpec  = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow) 
val  windowSpec2  = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow) 

val sum_rows= sum(col("Salary")).over(windowSpec)
val sum_range= sum(col("Salary")).over(windowSpec2)

val df=logical.select(col("RowID"), col("FName"), col("Salary") ,sum_rows.alias("SumByRows"),sum_range.alias("SumByRange") ).orderBy("RowID")
display(df)

// COMMAND ----------

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val sales_header = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM SalesLT.SalesOrderHeader")
  .load()

display(sales_header)

// COMMAND ----------

val sales_detail = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM SalesLT.SalesOrderDetail")
  .load()

display(sales_detail)

// COMMAND ----------

val windowSpec  = Window.partitionBy("AccountNumber").orderBy("OrderDate")

val row_nb= row_number().over(windowSpec)
val df=sales_header.select(col("AccountNumber"), col("OrderDate"), col("TotalDue") ,row_nb.alias("RN") ).orderBy("AccountNumber").limit(10)
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ex. 2

// COMMAND ----------

val windowSpec =Window.partitionBy(col("AccountId")).orderBy("TranDate")
val windowSpecRows=windowSpec.rowsBetween(Window.unboundedPreceding, -2) 


val tr_lead=lead(col("TranAmt"), 2).over(windowSpec)
val tr_first=first(col("TranAmt")).over(windowSpecRows)
val tr_last=last(col("TranAmt")).over(windowSpecRows)
val row_nb=row_number().over(windowSpec)
val tr_rank=dense_rank().over(windowSpec)

val df=transactions.select(col("*"), tr_lead,tr_first, tr_last, row_nb, tr_rank).orderBy("AccountId", "TranDate")
display(df)

// COMMAND ----------

val windowSpec =Window.partitionBy(col("AccountId")).orderBy("TranAmt")
val windowSpecRange=windowSpec.rangeBetween(-100 ,Window.currentRow) 

val tr_lead=lead(col("TranAmt"), 2).over(windowSpec)
val tr_first=first(col("TranAmt")).over(windowSpecRange)
val tr_last=last(col("TranAmt")).over(windowSpecRange)
val row_nb=row_number().over(windowSpec)
val tr_rank=dense_rank().over(windowSpec)

val df=transactions.select(col("*"),tr_lead, tr_first, tr_last, row_nb, tr_rank).orderBy("AccountId", "TranAmt")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ex. 3

// COMMAND ----------

val joinType = "leftsemi"
val joinExpression= sales_header.col("SalesOrderID") === sales_detail.col("SalesOrderID")
val result =sales_header.join(sales_detail, joinExpression, joinType)
result.explain()


// COMMAND ----------

display(result)

// COMMAND ----------

val joinType = "left_anti"

val sales_header_broken = sales_header.withColumn("SalesOrderID", when(col("SalesOrderID") === "71774", 99999).otherwise(col("SalesOrderID")))
val joinExpression= sales_header_broken.col("SalesOrderID") === sales_detail.col("SalesOrderID")
  
val result =sales_header_broken.join(sales_detail, joinExpression, joinType)

result.explain()

// COMMAND ----------

display(result)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ex. 4

// COMMAND ----------

// Drop after join
val joinExpression= sales_header.col("SalesOrderID") === sales_detail.col("SalesOrderID")
  
val result =sales_header.join(sales_detail, joinExpression).drop(sales_header.col("SalesOrderID")).select("SalesOrderID")
result.show()

// COMMAND ----------

// Renaming
val sales_detail2=sales_detail.withColumnRenamed("SalesOrderID", "order_id")

val joinExpression= sales_header.col("SalesOrderID") === sales_detail2.col("order_id")

val result =sales_header.join(sales_detail2, joinExpression).select("SalesOrderID", "order_id")
display(result)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ex. 5

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast
val joinExpr = sales_header.col("SalesOrderID") === sales_detail.col("SalesOrderID")
val result=sales_header.join(broadcast(sales_detail), joinExpr)
result.explain()


// COMMAND ----------

display(result)
