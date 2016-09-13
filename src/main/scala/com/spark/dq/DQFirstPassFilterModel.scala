package com.spark.dq

import org.apache.spark._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, Row, DataFrame}

/**
 * Created by Nagendra on 9/13/16.
 */
class DQFirstPassFilterModel(var path:String) extends Serializable {

  def += (df : DataFrame, sqlContext : SQLContext) : DataFrame = {

    val schema = df.schema
    val columnValueCounts = df.rdd.filter{r=> !getNonNullRecords(r,schema) }
    val filterDf = sqlContext.createDataFrame(columnValueCounts,df.schema).toDF();
    filterDf.write.save(path)
    filterDf

  }


  /**
   * Filter out the non null records
   * @param row
   * @param schema
   * @return
   */
  def getNonNullRecords(row: Row,schema: StructType) : Boolean = {
      (0 until schema.length).map { x =>
      if (row.get(x) == null) {
        return true
      }
    }
    return false
  }

}
