package org.example

import org.apache.spark.sql.SparkSession
import org.example.`case`.{Actor, Name}
import org.example.datareader.Datareader
import org.example.datawriter.Datawriter
import org.example.transformations.Transformations

object Project {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("app")
      .getOrCreate()

    val actors = Datareader.read(spark,args(0));
    val names = (Datareader.read(spark, args(1)));
    val actorsFiltered = actors.filter(row => Transformations.isCategory(row.asInstanceOf[Actor], "producer"))
    val namesFiltered = names.filter(row => Transformations.higherThan(row.asInstanceOf[Name], 165))

    actorsFiltered.show()
    namesFiltered.show()
    Datawriter.write(actorsFiltered, args(2));
    Datawriter.write(namesFiltered, args(3));
  }
}