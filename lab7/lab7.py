# Databricks notebook source
# MAGIC %md
# MAGIC Hive aktualnie nie wspiera indeksów.
# MAGIC 
# MAGIC Zgodnie z https://cwiki.apache.org/ alternatywami są:
# MAGIC 
# MAGIC * materialized views, wykorzystujące Apache Calcite do optymalizacji zapytań, porównujące stare i nowe tabele
# MAGIC * pliki kolumnowe - np. Parquet, OR

# COMMAND ----------

#Creating example db
movies_path = "dbfs:/FileStore/tables/Files/movies.csv"
db_name= "example"
spark.sql(f"CREATE DATABASE  IF NOT EXISTS {db_name}")
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(movies_path)

df.write.mode("overwrite").saveAsTable(f"{db_name}.movies")
df.write.mode("overwrite").saveAsTable(f"{db_name}.movies2")

# COMMAND ----------

print(spark.catalog.listTables(db_name))

def drop_all_tables_in_db(db :str, verbose=False):
    
    tables= spark.catalog.listTables(db)
    
    for  table in tables:
        db_table=f"{db}.{table.name}"
        
        if verbose:
            print(f"Deleting table {db_table}")
            
        spark.sql(f"DROP table {db_table}")

        
drop_all_tables_in_db(databases[1].name, verbose=True)
print(spark.catalog.listTables(db_name))

    
