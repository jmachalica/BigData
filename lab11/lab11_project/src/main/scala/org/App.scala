package org
import org.transformations.Filter
import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.sql.DataFrame
import org.case_classes.Flight
import org.data.{DataReader, DataWriter}


object App{

  def main(args: Array[String]) : Unit =
  {

    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Maven_first_app")
      .getOrCreate();

    import spark.implicits._
    val reader= new DataReader();

    val flights:DataFrame=reader.read_csv("C:\\Users\\hp\\Documents\\Studia\\3 rok\\6 semestr\\big_data\\lab11_project\\2015-summary.csv", spark.sqlContext, header = true );
    flights.show(10);

    val flights_dataset= flights.as[Flight];
    val filter=new Filter();


    val filtered=flights_dataset.filter(row => filter.countGt(row, 500))

    val writer=new data.DataWriter();
    writer.write(filtered.toDF(),"C:\\Users\\hp\\Documents\\Studia\\3 rok\\6 semestr\\big_data\\lab11_project\\summary.csv");


  }




}
