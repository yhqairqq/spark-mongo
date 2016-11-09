package com.unionpay.scala.app

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

/**
  * Created by yhqairqq@163.com on 16/9/29.
  */
object scalaSimple4 {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local")
      .setAppName("scalaSimple4")
    //    .setMaster("local")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sc)
    val people = sc.textFile("/Users/YHQ/python_pro/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/people.txt")
    //字符串格式表模式
    val schamaString = "name age"
    //导入依赖的数据类型
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.types.{StructType, StructField, StringType}
    //根据字符串格式表模式创建结构化的表模式,用StructType保存
    val schema = StructType(schamaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))


    //将rdd成员转换成row对象
    val rowRDD = people.map(_.split(" ")).map(p=>Row(p(0),p(1).trim))
    //将模式作用到RDD,生成dataFrame
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD,schema)
    //将DataFrame的内容打印到标准输出
    peopleDataFrame.show()


    val df = sqlContext.read.json("/Users/YHQ/python_pro/spark-2.0.0-bin-hadoop2.7/examples/src/main/resources/people.json")
    df.registerTempTable("people")
    df.show()
    val dfSchama = df.schema
    dfSchama.printTreeString()
    schema.printTreeString()
    dfSchama.fields.foreach(println)
    df.select("name").show()
    df.select(df("name"),df("age")+1).show()
    df.filter(df("age")>21).show()
    df.groupBy("age").count().show()
//    println(schema.equals(dfSchama))
    println("###################")
    val result = sqlContext.sql("select * from people")
    println(result)
    println("###################")






  }


}
