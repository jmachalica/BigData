# Databricks notebook source
# MAGIC %md Names.csv 
# MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
# MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
# MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
# MAGIC * Dodaj kolumnę i policz wiek aktorów 
# MAGIC * Usuń kolumny (bio, death_details)
# MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
# MAGIC * Posortuj dataframe po imieniu rosnąco

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *

def add_execution_time( df,execution_start):
  execution_end = datetime.now()
  return df.withColumn("execution_time", lit(execution_end.timestamp()-execution_start.timestamp()) )
  



# COMMAND ----------

# MAGIC %python
# MAGIC execution_start = datetime.now()
# MAGIC filePath = "dbfs:/FileStore/tables/Files/names.csv"
# MAGIC names_df = spark.read.format("csv")\
# MAGIC               .option("header","true")\
# MAGIC               .option("inferSchema","true")\
# MAGIC               .load(filePath)

# COMMAND ----------

names_df.explain(extended=True)

# COMMAND ----------

display(names_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)

# COMMAND ----------

#Previous version with UDF
# def cm_to_feet(cm):
#   if cm == None:
#     return None
#   return round(0.0328*cm,2)

# cm_to_feet_udf= udf(cm_to_feet, FloatType() )
# names_foot_df= names_df.withColumn("height_feet", cm_to_feet_udf(col("height")))
########


names_foot_df = names_df.withColumn("height_feet",round( col("height") *0.0328,2))

display(names_foot_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Odpowiedz na pytanie jakie jest najpopularniesze imię?

# COMMAND ----------

names_df.groupBy("name").count().orderBy(desc("count")).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Dodaj kolumnę i policz wiek aktorów 

# COMMAND ----------

birth_column=col("date_of_birth")
death_column=col("date_of_death")
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
birth_death_df= names_df.withColumn("birth",
    when(to_date(birth_column,"yyyy-MM-dd").isNotNull(),
           to_date(birth_column,"yyyy-MM-dd"))
    .when(to_date(birth_column,"yyyy MM dd").isNotNull(),
           to_date(birth_column,"yyyy MM dd"))
    . when(to_date(birth_column,"MM/dd/yyyy").isNotNull(),
           to_date(birth_column,"MM/dd/yyyy"))
    .when(to_date(birth_column,"yyyy MMMM dd").isNotNull(),
           to_date(birth_column,"yyyy MMMM dd"))
    .when(to_date(birth_column,"dd.MM.yyyy").isNotNull(),
           to_date(birth_column,"dd.MM.yyyy"))
  ).withColumn("death",
    when(to_date(death_column,"yyyy-MM-dd").isNotNull(),
           to_date(death_column,"yyyy-MM-dd"))
    .when(to_date(death_column,"yyyy MM dd").isNotNull(),
           to_date(death_column,"yyyy MM dd"))
    . when(to_date(death_column,"MM/dd/yyyy").isNotNull(),
           to_date(death_column,"MM/dd/yyyy"))
    .when(to_date(death_column,"yyyy MMMM dd").isNotNull(),
           to_date(death_column,"yyyy MMMM dd"))
    .when(to_date(death_column,"dd.MM.yyyy").isNotNull(),
           to_date(death_column,"dd.MM.yyyy"))
      .when(to_date(death_column,"yyyy").isNotNull(),
           to_date(death_column,"yyyy"))
  )

birth_death_df= birth_death_df.withColumn("age", when( col("death").isNotNull(),  round(months_between(col("death"),col("birth"), True)/12,0)  ). otherwise(round(months_between(current_date(),col("birth"), True)/12,0)))

                       
display(birth_death_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Usuń kolumny (bio, death_details)

# COMMAND ----------

cols_to_drop= ["bio", "death_details"] 
names_dropped_df=names_df.drop(*cols_to_drop)
display(names_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Zmień nazwy kolumn - dodaj kapitalizaję i usuń _

# COMMAND ----------

col_names=names_df.columns
def convert_name(old_name):
  old_name_parts=old_name.split("_")
  transformed=[ name.capitalize() if ind>0 else name for ind,name in enumerate(old_name_parts)  ]
  return "".join(transformed)

new_names= [ convert_name(oldname) for oldname in col_names ]
renamed_df=names_df.toDF(*new_names)
display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Posortuj dataframe po imieniu rosnąco

# COMMAND ----------

sorted_by_name=names_df.orderBy(asc("name"))
display(sorted_by_name)                                                     

# COMMAND ----------

df_execution= add_execution_time( names_df,execution_start)
df_execution.show(1)

# COMMAND ----------

# MAGIC %md Movies.csv
# MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
# MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
# MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
# MAGIC * Usuń wiersze z dataframe gdzie wartości są null

# COMMAND ----------

execution_start = datetime.now()
filePath = "dbfs:/FileStore/tables/Files/movies.csv"
movies_df= spark.read.format("csv")\
              .option("header","true")\
              .option("inferSchema","true")\
              .load(filePath)

display(movies_df)

# COMMAND ----------

date_column=col("date_published")
movies_date_replaced=movies_df.withColumn("date_published_replaced", when(date_column.contains('-') ,  regexp_replace(date_column,"\\-",".") ).otherwise(date_column ))

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
date_column=col("date_published_replaced")

movies_year_df=movies_date_replaced.withColumn("year_published", \
                                    when( to_date(date_column, "dd.MM.yyyy").isNotNull(), to_date(date_column, "dd.MM.yyyy")   )  \
                                    .when( to_date(date_column, "yyyy.MM.dd").isNotNull(), to_date(date_column, "yyyy.MM.dd")   )  \
                                    .when( to_date(date_column, "yyyy").isNotNull(), to_date(date_column, "yyyy")   )  \
                                      )         
movies_year_df=movies_year_df.withColumn( "years_since_publish",  round(months_between(current_date(),col("year_published"), True)/12,0))
display(movies_year_df)

# COMMAND ----------

movies_budget_df=movies_df.withColumn("budget_numeric", regexp_extract(col("budget"), r'\d+',0).cast(FloatType()) )
display(movies_budget_df)

# COMMAND ----------

movies_df_not_null=movies_df.na.drop()
display(movies_df_not_null)

# COMMAND ----------

df_execution= add_execution_time( movies_df,execution_start)
df_execution.show(1)

# COMMAND ----------

# MAGIC %md ratings.csv
# MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
# MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
# MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
# MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
# MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
# MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

# COMMAND ----------

execution_start= datetime.now()
filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
ratings_df = spark.read.format("csv")\
              .option("header","true")\
              .option("inferSchema","true")\
              .load(filePath)

display(ratings_df )

# COMMAND ----------

ratings_clean_df=ratings_df.na.drop()
display(ratings_clean_df)

# COMMAND ----------

start_col=ratings_clean_df.columns.index("votes_10")
end_col=ratings_clean_df.columns.index("votes_1")
print(start_col,end_col)

# COMMAND ----------

import statistics
def mean_row(*values):
   return statistics.mean(values)
mean_row=udf(mean_row, FloatType())

def median_row(*values):
   return statistics.median(values)
median_row=udf(median_row, FloatType())

columns=ratings_clean_df.select(ratings_clean_df.columns[start_col:end_col+1])
n_of_columns=len(columns.columns)

ratings_stat_df=ratings_clean_df.withColumn("mean",mean_row(*columns))
ratings_stat_df=ratings_stat_df.withColumn("median",median_row(*columns))

display(ratings_stat_df)

# COMMAND ----------

ratings_stat_diff_df= ratings_stat_df.withColumn("diff_mean", (col("weighted_average_vote") - col("mean") )).withColumn("diff_median", (col("weighted_average_vote") - col("median") ))
display(ratings_stat_diff_df)

# COMMAND ----------

#males_allages_avg_vote
#females_allages_avg_vote
stats=ratings_clean_df.select(mean(col("males_allages_avg_vote")).alias("men_mean"),mean(col("females_allages_avg_vote")).alias("females_mean")  )
stats.show() # females give higher average vote


# COMMAND ----------

ratings_clean_df=ratings_clean_df.withColumn("votes_7", col("votes_7").cast(LongType()))
ratings_clean_df.printSchema()

# COMMAND ----------

df_execution= add_execution_time( ratings_clean_df,execution_start)
df_execution.show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC Zadanie 2 
# MAGIC 
# MAGIC Przejdź przez Spark UI i opisz w kilku zdaniach co można znaleźć w każdym z elementów Spark UI. 

# COMMAND ----------

# MAGIC %md
# MAGIC **Spark UI**
# MAGIC 
# MAGIC 
# MAGIC **Jobs** 
# MAGIC 
# MAGIC Opis danego job, status, czas wykonania, timeline, DAG visualization,statusy stages
# MAGIC 
# MAGIC **Stages** 
# MAGIC 
# MAGIC Podsumowanie wykonanych stages
# MAGIC 
# MAGIC **Storage** 
# MAGIC 
# MAGIC Podsumowanie uzytej pamieci - odczyt i zapis z roznych zrodel
# MAGIC 
# MAGIC **Environment**
# MAGIC 
# MAGIC Podsumowanie wlasciwosci srodowiska -> opis wlasciwosci sparka, hadoopa, systemowych
# MAGIC 
# MAGIC **Executors**
# MAGIC 
# MAGIC Opis spark executors -> wlasciwosci drivera
# MAGIC 
# MAGIC 
# MAGIC **SQL**
# MAGIC 
# MAGIC podsumowanie wykonanych queries
# MAGIC 
# MAGIC **JDBC/ODBC Server**
# MAGIC 
# MAGIC podsumowanie serwera, ilosc aktywnych sesji i wykonywanych zapytan sql
# MAGIC 
# MAGIC 
# MAGIC **structured streaming**
# MAGIC 
# MAGIC UI opisujace przetwarzanie strumieniowe

# COMMAND ----------

# MAGIC %md
# MAGIC Zadanie 3

# COMMAND ----------

filePath = "dbfs:/FileStore/tables/Files/names.csv"
names_df = spark.read.format("csv")\
              .option("header","true")\
              .option("inferSchema","true")\
              .load(filePath)
display(names_df)

# COMMAND ----------

names_df.groupBy("divorces").count().explain(extended=True)

# COMMAND ----------

names_df.select("*").explain(extended=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Zadanie 4
# MAGIC 
# MAGIC Pobierz dane z SQL Server przynajmniej jedna tabelę.
# MAGIC 
# MAGIC Sprawdź jakie są opcje zapisu danych przy użyciu konektora jdbc. 

# COMMAND ----------

# MAGIC %python
# MAGIC database_name = "database_name"
# MAGIC username="sqladmin"
# MAGIC url="jdbc:sqlserver://bidevtestserver.database.windows.net;databaseName=testdb;"
# MAGIC password="$3bFHs56&o123$"
# MAGIC 
# MAGIC try:
# MAGIC   tables_df=spark.read.format("jdbc") \
# MAGIC   .option("url", url) \
# MAGIC   .option("dbtable", "INFORMATION_SCHEMA.TABLES" ) \
# MAGIC   .option("user", username) \
# MAGIC   .option("password", password) \
# MAGIC   .load()
# MAGIC except ValueError as error :
# MAGIC   print("Connector write failed", error)

# COMMAND ----------

# MAGIC %python
# MAGIC tables_df.show()

# COMMAND ----------

try:
  customer_df=spark.read.format("jdbc") \
  .option("url", url) \
  .option("dbtable", "SalesLT.Customer" ) \
  .option("user", username) \
  .option("password", password) \
  .load()
  display(customer_df)
except ValueError as error :
  print("Connector write failed", error)

# COMMAND ----------

display(names_df)
