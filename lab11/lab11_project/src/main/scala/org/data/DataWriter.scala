package org.data

import org.apache.spark.sql.{DataFrame, Dataset}

class DataWriter {

  def write(df:DataFrame,path:String, format: String = "csv"): Unit = {
    df.write.format(format).save(path);
  }


}
