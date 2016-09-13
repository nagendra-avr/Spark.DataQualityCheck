package com.spark.dq

import org.apache.spark.sql.{DataFrame}

/**
 * Created by Nagendra on 9/13/16.
 */
class DQThirdPassErrorFileModel(errorFile : String) {

  def += (df : DataFrame) : Unit = {

    val schema = df.schema;
    val fieldnames = schema.fieldNames

    val columnValue = df.flatMap(r =>
      (0 until schema.length).map { idx =>
        //((columnName), cellValue)
       ((fieldnames(idx).toString), if (r.get(idx) == null) "null" else r.get(idx))
      }
    ).saveAsHadoopFile(errorFile, classOf[String], classOf[String],
      classOf[RDDMultipleTextOutputFormat])
  }
}
