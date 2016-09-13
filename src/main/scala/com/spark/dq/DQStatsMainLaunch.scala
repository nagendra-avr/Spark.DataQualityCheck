package com.spark.dq

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Nagendra on 9/13/16.
 */
object DQStatsMainLaunch {

  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("DQStatsMainLaunch <inputPath outputpath errorFilePath>")
      return
    }

      val inputPath = args(0)
      val outputPath = args(1)
      val errorFilePath = args(2)
      var sc:SparkContext = null

      val sparkConfig = new SparkConf().setAppName("TableStatsSinglePathMain").setMaster("local")
      sc = new SparkContext(sparkConfig)

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val df = sqlContext.read.load(inputPath)

      val firstPassFilterModel = new DQFirstPassFilterModel(outputPath);

      val filterDf = firstPassFilterModel +=(df,sqlContext)
      filterDf.show()
      val secondPassSubstractModel = new  DQSecondPassSubstractModel

      val nullDF = secondPassSubstractModel += (df,filterDf,sqlContext)
      nullDF.show

      val thirdPassErrorFileModel = new DQThirdPassErrorFileModel(errorFilePath)

      thirdPassErrorFileModel += (nullDF)

      sc.stop()
    }

}
