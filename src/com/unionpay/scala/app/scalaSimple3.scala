package com.unionpay.scala.app

import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, BasicBSONObject}

import scala.collection.mutable

/**
  * Created by yhqairqq@163.com on 16/9/29.
  */
object scalaSimple3 {
  def square(x:Int)={x*x}
  def main(args: Array[String]) {
    val result = square(new Integer(12))
    println(result)


    val set = mutable.Set()
    val votes = Seq(("scala", 1), ("java", 4), ("scala", 10), ("scala", 1), ("python", 10))
    val orderedVotes = votes
      .groupBy(_._1)
      .map { case (which, counts) =>
        (which, counts.foldLeft(0)(_ + _._2))
      }.toSeq
      .sortBy(_._2)
      .reverse
    orderedVotes.foreach(println)

  }


}
