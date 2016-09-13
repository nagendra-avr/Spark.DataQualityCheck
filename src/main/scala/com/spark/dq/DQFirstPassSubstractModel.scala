package com.spark.dq

import org.apache.spark.sql.{SQLContext, DataFrame}

/**
 * Created by nagi on 9/13/16.
 */
class DQFirstPassSubstractModel {

  def += (rawdf : DataFrame, records : DataFrame,sqlContext : SQLContext) : DataFrame = {

    val schema = rawdf.schema
    val columnValueCounts = rawdf.rdd.subtract(records.rdd)
    val nullDF = sqlContext.createDataFrame(columnValueCounts,schema).toDF()
    return nullDF

  }
}
