package com.unionpay.scala.app

import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, BasicBSONObject}
/**
  * Created by yhqairqq@163.com on 16/9/29.
  */
object scalaSimple1 extends App{
  // Set up the configuration for reading from MongoDB.
  val mongoConfig = new Configuration()
  // MongoInputFormat allows us to read from a live MongoDB instance.
  // We could also use BSONFileInputFormat to read BSON snapshots.
  // MongoDB connection string naming a collection to read.
  // If using BSON, use "mapred.input.dir" to configure the directory
  // where the BSON files are located instead.
//  mongoConfig.set("mongo.input.uri",
//    "mongodb://127.0.0.1:33332/crawl_hz.MainShop2")
  mongoConfig.set("mongo.input.uri",
    "mongodb://127.0.0.1:33332/crawl_hz.MainShop2")

  val sparkConf = new SparkConf().setMaster("local")
    .setAppName("scalaSimple1")
//    .setMaster("local")
  val sc = new SparkContext(sparkConf)

  // Create an RDD backed by the MongoDB collection.
  val documents = sc.newAPIHadoopRDD(
    mongoConfig,                // Configuration
    classOf[MongoInputFormat],  // InputFormat
    classOf[Object],            // Key type
    classOf[BSONObject])        // Value type

//  documents.collect().foreach(println)
  // Input contains tuples of (ObjectId, BSONObject)
  val countsRDD = documents.flatMap(arg => {
      var str = arg._2.get("description").toString
      str = str.toLowerCase().replaceAll("[.,!?\n]", " ")
      str.split(" ")
    })
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

//  countsRDD.collect().foreach(println)

  val result = countsRDD.map((x)=>{
//    (x._1)
    val bson = new BasicBSONObject()
    bson.put("word",x._1)
    bson.put("count",new Integer(x._2))
    (null,bson)
  })

//    result.collect().foreach(println)



  // Create a separate Configuration for saving data back to MongoDB.
  val outputConfig = new Configuration()
//  outputConfig.set("mongo.output.uri",
//    "mongodb://127.0.0.1:33332/crawl_hz.MainShop3")
  outputConfig.set("mongo.output.uri",
    "mongodb://127.0.0.1:33332/crawl_hz.MainShop3")

//   Save this RDD as a Hadoop "file".
//   The path argument is unused; all documents will go to "mongo.output.uri".
  result.saveAsNewAPIHadoopFile(
    "file:///this-is-completely-unused",
    classOf[Object],
    classOf[BSONObject],
    classOf[MongoOutputFormat[Object, BSONObject]],
    outputConfig)

  // We can also save this back to a BSON file.
//  val bsonOutputConfig = new Configuration()
//  documents.saveAsNewAPIHadoopFile(
//    "hdfs://localhost:8020/user/spark/bson-demo",
//    classOf[Object],
//    classOf[BSONObject],
//    classOf[BSONFileOutputFormat[Object, BSONObject]])

  // We can choose to update documents in an existing collection by using the
  // MongoUpdateWritable class instead of BSONObject. First, we have to create
  // the update operations we want to perform by mapping them across our current
  // RDD.
//  updates = documents.mapValues(
//    value => new MongoUpdateWritable(
//      new BasicDBObject("_id", value.get("_id")),  // Query
//      new BasicDBObject("$set", new BasicDBObject("foo", "bar")),  // Update operation
//      false,  // Upsert
//      false   // Update multiple documents
//    )
//  )
//
//  // Now we call saveAsNewAPIHadoopFile, using MongoUpdateWritable as the
//  // value class.
//  updates.saveAsNewAPIHadoopFile(
//    "file:///this-is-completely-unused",
//    classOf[Object],
//    classOf[MongoUpdateWritable],
//    classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
//    outputConfig)

}
