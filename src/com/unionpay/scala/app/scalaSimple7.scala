package com.unionpay.scala.app

import com.mongodb.hadoop.MongoInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject
import com.mongodb.spark._
import  com.mongodb.spark.sql.DefaultSource

/**
  * Created by yhqairqq@163.com on 16/9/29.
  */
object scalaSimple7 {
  def main(args: Array[String]) {

    val mongoConfig = new Configuration()

    mongoConfig.set("mongo.input.uri",
      "mongodb://127.0.0.1:33332/crawl_hz.MainShop2")

    val sparkConf = new SparkConf().setMaster("local")
      .setAppName("scalaSimple7")
    //    .setMaster("local")
    val sc = new SparkContext(sparkConf)

    val documents = sc.newAPIHadoopRDD(
      mongoConfig, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject]) // Value type

    val sqlContext = SQLContext.getOrCreate(sc)  // sc is an existing SparkContext



    val df = MongoSpark.load(sqlContext)  // Uses the SparkConf
    df.printSchema()


  }


}
