package org.data

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}

class DataReader {

  def read_csv(path: String, sql_context: SQLContext, schema: StructType = null, header : Boolean=false): DataFrame = {

    var options = sql_context.read.format("csv").option("header", header)

    if (schema != null) {
      options = options.schema(schema);
    }
    else{
      options= options.option("inferSchema", true);
    }


    val final_options = if (schema != null) options else options.schema(schema);

    return final_options.load(path);
  }


}
