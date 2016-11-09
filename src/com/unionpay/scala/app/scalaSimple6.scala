package com.unionpay.scala.app

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yhqairqq@163.com on 16/9/29.
  */
object scalaSimple6 {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local")
      .setAppName("scalaSimple6")
    //    .setMaster("local")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("json").load("/Users/YHQ/python_pro/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/people.json")

    df.show()




  }


}
