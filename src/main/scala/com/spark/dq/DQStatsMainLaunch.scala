package com.spark.dq

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Nagendra on 9/13/16.
 */
object DQStatsMainLaunch {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("DQStatsMainLaunch <inputPath outputpath>")
      return
    }

      val inputPath = args(0)
      val outputPath = args(1)
      var sc:SparkContext = null

      val sparkConfig = new SparkConf().setAppName("TableStatsSinglePathMain").setMaster("local")
      sc = new SparkContext(sparkConfig)

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val df = sqlContext.read.load(inputPath)

      val firstPassFilterModel = new DQFirstPassFilterModel(outputPath);

      val filterDf = firstPassFilterModel +=(df,sqlContext)
      filterDf.show()
      val firstPassSubstractModel = new  DQFirstPassSubstractModel

      val nullDF = firstPassSubstractModel += (df,filterDf,sqlContext)
      nullDF.show
      sc.stop()
    }

}
