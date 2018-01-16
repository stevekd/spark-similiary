package com.bbtree.spark.httpclient
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ListBuffer
/**
  * Created by lenovo on 2018/1/15 0015.
  */
object test_dataframe {
  def getCollaborateSource2(str1: String, str2: String ): Double = {
    val user1 = str1.split(",").map(_.toDouble).toVector
    val user2 = str2.split(",").map(_.toDouble).toVector
    val member = user1.zip(user2).map(d => d._1 * d._2).reduce(_ + _) //对公式分子部分进行计算
    val temp1 = math.sqrt(user1.map(num => { //求出分母第1个变量值
      math.pow(num, 2) //数学计算
    }).reduce(_ + _)) //进行叠加
    val temp2 = math.sqrt(user2.map(num => { ////求出分母第2个变量值
      math.pow(num, 2) //数学计算
    }).reduce(_ + _)) //进行叠加
    val denominator = temp1 * temp2 //求出分母
    member / denominator //进行计算
  }
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("UsersSimilaritySpark ") //设置环境变量
    val sc = new SparkContext(conf) //实例化环境
    val sqlContext = new SQLContext(sc)
    val data = sqlContext.createDataFrame(List((20,1,2,4),(23,2,2,5),
      (34,5,43,89),(36,7,8,7))).toDF("ID","tag1", "tag2", "tag3")

    val assembler = new VectorAssembler()
      .setInputCols(Array("tag1", "tag2", "tag2"))
      .setOutputCol("features")
    val output = assembler.transform(data)
    val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val scalerModel = scaler.fit(output)                //dataframe输入的数据
    val scaledData = scalerModel.transform(output)      //dataframe输入的数据
    scaledData.show()
    val allyou=scaledData.unionAll(scaledData)
    allyou.show()

    val rdd = allyou.select("ID","scaledFeatures").rdd
    println(rdd.count)
    println(rdd.count())
   val nba= scaledData.count().toInt
    println(nba)



    val list = new ListBuffer[(Int, String)]
    val map: RDD[ListBuffer[(Int, String)]] = rdd.map(x => {
      list.append((x.getAs[Int]("ID"),x.get(1).toString.substring(1,(x.get(1).toString.length-1))))
      list
    }).distinct()
    println("map.collect()")
    println(map.collect().toBuffer)



    val map1: RDD[String] = map.map(list => {
      var list_content = new ListBuffer[String]
      for (i <- 0 until nba) {
        for (j <- 0 until list.length) {
          if (i != j) {
            list_content.append(list(i)._1.toString +","+ list(j)._1.toString +","+ getCollaborateSource2(list(i)._2, list(j)._2).toString)
          }
        }
      }
      list_content
    }).flatMap(x => {x})
    println(map1.collect().toBuffer)

    //RDD 2 DataFrame
    val  resultRDD = map1.map(_.split(",")).map(x =>Row(x(0),x(1),x(2)))
    println(resultRDD.collect().toBuffer)
    //resultRDD.coalesce(1,true).saveAsTextFile("./text4.txt")
    // val schemaString = "userID newUserID similarity"
    // val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val schema=StructType(
      StructField("userId", StringType, true) ::
        StructField("newUserID", StringType, true) ::
        StructField("similarity",StringType,true) ::Nil)
    val  peopleSchemaRDD = sqlContext.createDataFrame(resultRDD,schema)
    val peopleSchemaRDD2=peopleSchemaRDD.na.fill("0")
    peopleSchemaRDD2.show()
    //降序排列
    peopleSchemaRDD.sort(peopleSchemaRDD("userId").desc, peopleSchemaRDD("similarity").desc)
    peopleSchemaRDD.sort(peopleSchemaRDD("userId").desc, peopleSchemaRDD("similarity").desc).show()
    peopleSchemaRDD.show()
//    //save result
//    val hiveContext=new  HiveContext(sc)
//    import hiveContext.implicits._
//    hiveContext.sql("use DataBaseName")
//    peopleSchemaRDD.registerTempTable("table1")
//    // use sql in dataframe
//    val sortedByNameEmployees = sqlContext.sql("select * from table1 order by name desc")
//    sortedByNameEmployees.show()
//    hiveContext.sql("insert into table2 partition(date='2015-04-02') select userId,newUserID,similarity from table1")
val time=NowDate()
    println("now the time is "+time+"")
  }
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val date = dateFormat.format(now)
     date
  }
}
