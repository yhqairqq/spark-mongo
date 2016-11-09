package com.unionpay.scala.app

import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import com.mongodb.spark.sql.fieldTypes.ObjectId
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, BasicBSONObject}

/**
  * Created by yhqairqq@163.com on 16/9/29.
  */
object scalaSimple9 {
  def main(args: Array[String]) {


    val isOnLine = false

    val mongoConfig = new Configuration()
    if (isOnLine)
      mongoConfig.set("mongo.input.uri",
        "mongodb://10.15.159.169:30000/crawl_hz.MainShop2")
    else
      mongoConfig.set("mongo.input.uri",
        "mongodb://127.0.0.1:33332/crawl_hz.MainShop2")

    //  val objectId = new  ObjectId("57eb6ff06f42295e93a05641")
    /**
      * 设置查询条件详见
      * http://stackoverflow.com/questions/27523337/how-to-query-to-mongo-using-spark
      */
//    val query = "{\"_id\":{$gt:ObjectId(\"57a029fc951f59b1cf85730a\")}}"
//    val bson = new BasicBSONObject()
//    bson.put("age", new Integer(13))
//    println(query.toString)


//   println( ObjectId("57a029fc951f59b1cf85730a").toString)
//    mongoConfig.set("mongo.input.query", query)
////    mongoConfig.set("mongo.input.sort", "{\"_id\":1}")
////    mongoConfig.set("mongo.input.limit", "10")
//    mongoConfig.set("mongo.input.query", query)
    val sparkConf = new SparkConf() //.setMaster("local")
      .setAppName("scalaSimple9")
      .setMaster("local")
    //

//    val seq = Seq(bson.toString, bson.toString)


    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc);

    // Create an RDD backed by the MongoDB collection.
    val documents = sc.newAPIHadoopRDD(
      mongoConfig, // Configuration
      classOf[MongoInputFormat], // InputFormat
      classOf[Object], // Key type
      classOf[BSONObject]) // Value type


//    println("Documents: " + documents.count())
    //    documents.foreach(println)

    //      documents.collect().foreach(println)
    // Input contains tuples of (ObjectId, BSONObject)
    val countsRDD1 = documents.flatMap(arg => {
      var str = arg._2.get("desc").toString
      str = str.toLowerCase().replaceAll("[.,!?\n]", " ")
      str.split(" ")
    })
    countsRDD1.collect().foreach(println)

   val  countsRDD2=  countsRDD1.map(word => (word, 1))
    countsRDD2.collect().foreach(println)
   val  countsRDD3= countsRDD2.reduceByKey((a, b) => a + b)
    countsRDD3.collect().foreach(println)

    //
    //
    val result = countsRDD3.map((x) => {
      //    (x._1)
      val bson = new BasicBSONObject()
      bson.put("word", x._1)
      bson.put("count", new Integer(x._2))
      (null, bson)
    })
    //
    //    //    result.collect().foreach(println)
    //
    //
    //
    //    // Create a separate Configuration for saving data back to MongoDB.
    val outputConfig = new Configuration()
    if (isOnLine)
      outputConfig.set("mongo.output.uri",
        "mongodb://10.15.159.169:30000/crawl_hz.MainShop3")
    else
      outputConfig.set("mongo.output.uri",
        "mongodb://127.0.0.1:33332/crawl_hz.MainShop3")

    //
    //    //   Save this RDD as a Hadoop "file".
    //    //   The path argument is unused; all documents will go to "mongo.output.uri".
    result.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[BSONObject],
      classOf[MongoOutputFormat[Object, BSONObject]],
      outputConfig)
  }


}
