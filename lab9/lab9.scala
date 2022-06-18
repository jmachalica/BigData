// Databricks notebook source
var path = "/FileStore/tables/Nested.json";
var df= spark.read. option("multiline", "true").json(path)

display(df)

// COMMAND ----------

display(df.withColumn("pathLinkInfoEdited", $"pathLinkInfo". dropFields("captureSpecification", "elevationGain")))

// COMMAND ----------

display(df.withColumn("pathLinkInfoEdited", $"pathLinkInfo". dropFields("captureSpecification", "elevationGain.elevationInDirection")))

// COMMAND ----------

import org.apache.spark.sql.functions._

val sourceDF = Seq(
  ("  p a   b l o", "Paraguay"),
  ("Neymar", "B r    asil")
).toDF("name", "country")

display(sourceDF)



// COMMAND ----------

val actualDF = sourceDF
  .columns
  .foldLeft(sourceDF) { (memoDF, colName) =>
    memoDF.withColumn(
      colName,
   regexp_replace(col(colName), "\\s+", "")
    )
  }

display(actualDF)


// COMMAND ----------

val df_renamed_columns = df.columns.foldLeft(df){(df, col_name)=>
  
  df.withColumnRenamed(col_name, col_name.toLowerCase()
)
}

display(df_renamed_columns)

// COMMAND ----------

val numbers = List(5, 4, 8, 6, 2)
numbers.foldLeft(0 ) { (z, i) =>
  z + i
}


// COMMAND ----------

numbers.foldLeft(numbers) { (r,c) => c :: r }

// COMMAND ----------

class Foo(val name: String, val age: Int, val sex: Symbol)

object Foo {
  def apply(name: String, age: Int, sex: Symbol) = new Foo(name, age, sex)
}

val fooList = Foo("Hugh Jass", 25, 'male) ::
              Foo("Biggus Dickus", 43, 'male) ::
              Foo("Incontinentia Buttocks", 37, 'female) ::
              Nil

fooList.foldLeft(List[String]()) { (z, f) =>
  val title = f.sex match {
    case 'male => "Mr."
    case 'female => "Ms."
  }
  z :+ s"$title ${f.name}, ${f.age}"
}

// COMMAND ----------

val excluded_fields = Map("pathLinkInfo" -> Array("captureSpecification","cycleFacility"), "new_column"-> Array("elevationGain.elevationInDirection") )


display(excluded_fields.foldLeft(df){ (k,v) => (
                            k.withColumn("new_column", col(v._1).dropFields(v._2:_*)))
                            }.select("pathLinkInfo", "new_column"))

