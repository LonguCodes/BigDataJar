package org.example.datawriter

import org.apache.spark.sql.DataFrame

object Datawriter {
  def write(dataframe: DataFrame, path: String, format: String = "csv"){
    dataframe.write.format(format).save(path)
  }
}
