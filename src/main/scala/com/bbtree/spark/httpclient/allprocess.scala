package com.bbtree.spark.httpclient

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD

//import hiveContext.implicits._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.rdd.RDD
//import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.VectorAssembler
import java.sql.DriverManager
import java.sql.Connection
/**
  * Created by lenovo on 2018/1/12 0012.
  */
object allprocess {
  //两两计算分值,采用余弦相似性
  def NowDate(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val date = dateFormat.format(now)
    date
  }

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
   // val conf = new SparkConf().setAppName("UsersSimilaritySpark")
    val sc = new SparkContext(conf) //实例化环境
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    val nowtime:String=NowDate()
    //spark SQL 查询
    val see_infoRDD = hiveContext.sql("""
    select distinct a.user_id ,a.user_role,a.school_id,a.call_grade,
    a.is_graduation,a.sex_code,a.province_id,a.city_id,a.area_id
    from label.label_user a
    left join (select distinct user_id from pdw.pdw_user_browse_log where dt='"""+nowtime+"""' )b
    on a.user_id=b.user_id
    where a.dt='"""+nowtime+"""' and b.user_id is not null
    """).toDF("user_id", "user_role", "school_id" ,"call_grade",
      "is_graduation","sex_code", "province_id", "city_id", "area_id")

    val nosee_infoRDD = hiveContext.sql("""
    select distinct a.user_id ,a.user_role,a.school_id,
    a.call_grade,a.is_graduation,a.sex_code,a.province_id,a.city_id,a.area_id
    from label.label_user   a
    left join (select distinct user_id from pdw.pdw_user_browse_log where dt='"""+nowtime+"""'  )b
    on a.user_id=b.user_id
    where a.dt='"""+nowtime+"""'and b.user_id is  null
    """).toDF("user_id", "user_role", "school_id" ,"call_grade",
      "is_graduation","sex_code", "province_id", "city_id", "area_id")

    ///多列合并成一列
    val assembler = new VectorAssembler()
      .setInputCols(Array("school_id" ,"call_grade", "is_graduation","sex_code", "province_id", "city_id", "area_id"))
      .setOutputCol("features")
    val output_see = assembler.transform(see_infoRDD)
    val output_nosee = assembler.transform(nosee_infoRDD)
    output_see.show()

    ////归一化计算
    val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")

    val see_scalerModel = scaler.fit(output_see)                          // output 输入的数据
    val see_scaledData = see_scalerModel.transform(output_see)            // output 输入的数据

    val nosee_scalerModel = scaler.fit(output_nosee)                      // output 输入的数据
    val nosee_scaledData = nosee_scalerModel.transform(output_nosee)      // output 输入的数据

    val all_scaledData=see_scaledData.unionAll(nosee_scaledData)
    //    scaledData.drop(scaledData("features"))
//    scaledData.show()
//    scaledData.select("ID","scaledFeatures").show()
    /// 计算相似性
    //  import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
    //  val showcos= new  org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix.columnSimilarities()
//    val map10: RDD[(String, String)] = scaledData["scaledFeatures"].map(x => {
//      (x.keys.mkString, x.values.mkString)
//    })
//    def cos(vec1:DoubleMatrix,vec2:DoubleMatrix):Double={
//      vec1.dot(vec2)/(vec1.norm2()*vec2.norm2())
//    }
    //see_scaledData  length
    val lensee:Int=see_scaledData.count().toInt

    //dataframe to rdd
    val allrdd=all_scaledData.select("user_id","scaledFeatures").rdd
    println("all.rdd and mapnosee.rdd")

    //calculate  the similarity
    val list = new ListBuffer[(Int, String)]
    val maps1: RDD[ListBuffer[(Int, String)]] = allrdd.map(x => {
      list.append((x.getAs[Int]("user_id"),x.get(1).toString.substring(1,(x.get(1).toString.length-1))))
      list
    }).distinct()

    val map1: RDD[String] = maps1.map(list => {
      var list_content = new ListBuffer[String]
      for (i <- lensee until list.length) {
        for (j <- 0 until lensee) {
          if (i != j) {
            list_content.append(list(i)._1.toString +","+ list(j)._1.toString +","+ getCollaborateSource2(list(i)._2, list(j)._2).toString)
          }
        }
      }
      list_content
    }).flatMap(x => {x})
    println("Finish the similarity")
    //rdd to dataframe
    val  resultRDD = map1.map(_.split(",")).map(x =>Row(x(0),x(1),x(2)))
    println(resultRDD.collect().toBuffer)
    val schema=StructType(
      StructField("NewUserID", StringType, true) ::
        StructField("OldUserID", StringType, true) ::
        StructField("similarity",StringType,true) ::Nil)
    val  peopleSchemaRDD = sqlContext.createDataFrame(resultRDD,schema)
    val peopleSchemaRDD2=peopleSchemaRDD.na.fill("0")
    println("Finish the rdd to dataframe")
    //sort the order


    //save result
    import hiveContext.implicits._
    hiveContext.sql("use liufei")
    hiveContext.sql(
      """
        |CREATE  TABLE if not exists liufei.user_similiary (
        |  NewUserID                STRING COMMENT '新来的用户ID',
        |  OldUserID                STRING COMMENT '观看过内容的用户ID',
        |  similiary                STRING  COMMENT '相似度'
        |)
        |PARTITIONED BY (
        |dt STRING
        |)
        |row format delimited fields terminated by ','
        |STORED AS TEXTFILE
        |LOCATION 'hdfs://nameservice1/user/hive/warehouse/liufei.db/user_similiary'
      """.stripMargin)
    //register
    peopleSchemaRDD.sort(peopleSchemaRDD("NewUserID").desc, peopleSchemaRDD("similarity").desc)
      .registerTempTable("user_similarity_temp")
    // insert into table

    hiveContext.sql("insert into user_similarity partition(dt='"+nowtime+"') select NewUserID,OldUserID,similarity from user_similarity_temp")
  }

}