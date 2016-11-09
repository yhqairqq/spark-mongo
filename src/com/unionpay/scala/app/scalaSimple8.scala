package com.unionpay.scala.app

import com.mongodb.hadoop.MongoInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.BSONObject

/**
  * Created by yhqairqq@163.com on 16/9/29.
  */
object scalaSimple8 {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local")
      .setAppName("scalaSimple8")
    //    .setMaster("local")
    val sc = new SparkContext(sparkConf)

    //创建streamingContext,基于shell下默认的SparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    //创建DStream ,这是Streaming计算下的RDD
    val lines = ssc.socketTextStream("127.0.0.1", 9999)

    //基本的操作函数与rdd同名
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)


    //打印结果到标准输出,只打印DStream中每个RDDqia前10个元素
    //主意,print 不同于rdd中的action操作,不会触发真正的调度执行
    wordCounts.print()


    //这里才正式启动计算
    ssc.start()

    //等待执行结束(出错活着ctrl+c推出)
    ssc.awaitTermination()

    ssc.stop()
  }



}
