package com.bbtree.spark.httpclient
import java.util
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

//import hiveContext.implicits._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.{DataFrame, SQLContext, Row}
import org.apache.spark.ml.feature.MinMaxScaler
import java.util.Arrays
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
/**
  * Created by lenovo on 2018/1/12 0012.
  */
object testRDD {
  //两两计算分值,采用余弦相似性
  def getCollaborateSource2(str1: String, str2: String ): Double = {
    val user1 = str1.split(",").map(_.toInt).toVector
    val user2 = str2.split(",").map(_.toInt).toVector
    val member = user1.zip(user2).map(d => d._1.toInt * d._2.toInt).reduce(_ + _).toDouble //对公式分子部分进行计算
    val temp1 = math.sqrt(user1.map(num => { //求出分母第1个变量值
      math.pow(num.toInt, 2) //数学计算
    }).reduce(_ + _)) //进行叠加
    val temp2 = math.sqrt(user2.map(num => { ////求出分母第2个变量值
      math.pow(num.toInt, 2) //数学计算
    }).reduce(_ + _)) //进行叠加
    val denominator = temp1 * temp2 //求出分母
    member / denominator //进行计算
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("UsersSimilaritySpark ") //设置环境变量
    val sc = new SparkContext(conf) //实例化环境
    val sqlContext = new SQLContext(sc)

    val map: RDD[Map[String, String]] = sc.textFile("./text3.txt").map(line => {
      var m2 = scala.collection.immutable.Map[String, String]()
      m2 += (line.split(",")(0) -> (line.split(",")(1)+","+line.split(",")(2)+","+line.split(",")(3)))
      m2
    })

    val map1: RDD[(String, String)] = map.map(x => {
      (x.keys.mkString, x.values.mkString)
    })

    val list = new ListBuffer[(String, String)]
    val map2: RDD[ListBuffer[(String, String)]] = map1.map(x => {
      list.append(x)
      list
    }).distinct()
    println(map2.collect().toBuffer)
    val map3: RDD[String] = map2.map(list => {
      var list_content = new ListBuffer[String]
      for (i <- 0 until list.length) {
        for (j <- 0 until list.length) {
          if (i != j) {
            list_content.append(list(i)._1 + "," + list(j)._1 + "," + getCollaborateSource2(list(i)._2, list(j)._2))
          }
        }
      }
      list_content
    }).flatMap(x => {x})
    println(map3.collect().toBuffer)
    //RDD 2 DataFrame
    val  resultRDD = map3.map(_.split(",")).map(x =>Row(x(0),x(1),x(2)))
    println(resultRDD.collect().toBuffer)
    //resultRDD.coalesce(1,true).saveAsTextFile("./text4.txt")
   // val schemaString = "userID newUserID similarity"
   // val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val schema=StructType(
          StructField("userId", StringType, true) ::
          StructField("newUserID", StringType, true) ::
          StructField("similarity",StringType,true) ::Nil)
    val  peopleSchemaRDD = sqlContext.createDataFrame(resultRDD,schema)
    peopleSchemaRDD.show()
    //降序排列
    peopleSchemaRDD.sort(peopleSchemaRDD("userId").desc, peopleSchemaRDD("similarity").desc).show()
  // insert DataFrame  to  hive parition table


  }
}