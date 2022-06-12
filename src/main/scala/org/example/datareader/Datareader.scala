package org.example.datareader

import org.apache.spark.sql.{DataFrame, SparkSession}

object Datareader {
   def read(spark: SparkSession, path: String, format: String = "csv", header:Boolean = true): DataFrame = {
    val data = spark.read.format(format).option("header", header).option("inferSchema", value = true).load(path)
    return data.toDF();
  }
}
