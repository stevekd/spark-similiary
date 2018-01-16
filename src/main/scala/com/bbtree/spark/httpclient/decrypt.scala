package com.bbtree.spark.httpclient

import java.text.SimpleDateFormat

import com.bbtree.AESTools
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}


/*
* Created by chenzhilei on 16 / 5 / 31.
*/


object decrypt {


  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: userTrack <source_file>  <target_file> <partitions>")
      System.exit(1)
    }
    user_track(args)

  }

  def user_track(args: Array[String]): Unit = {
    val Array(source_file, target_file, partitions) = args

    val conf = new SparkConf().setAppName("decrypt log")
    val sc = new SparkContext(conf)
    val file = sc.textFile(source_file).cache().repartition(partitions.toInt)
    file
      .map(l => AESTools.format(l))
      .filter(x => {
        val c = x.split("\001")
//        println("解密后数据大小"+c.size)
        if (c.size > 19) {
          var request = ""
          var request_url = ""
          try {
            request = x.split("\001")(5)
            request_url = request.split(" ")(1)
          } catch {
            case ex: Exception => println("ERROR,异常了"+x)// Handle other I/O error
          }
          !(request_url.endsWith(".js") || request_url.endsWith(".css") || request_url.endsWith(".jpg"))
        } else
          false
      })
      .saveAsTextFile(target_file)
  }
}
