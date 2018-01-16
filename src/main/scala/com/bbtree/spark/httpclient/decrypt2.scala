package com.bbtree.spark.httpclient

import java.util.zip.GZIPInputStream

import com.bbtree.AESTools
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.{TextInputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}


/*
* Created by chenzhilei on 16 / 5 / 31.
*/


object decrypt2 {


  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: userTrack <source_file>  <target_file> <partitions>")
      System.exit(1)
    }
    user_track(args)

  }


  //  case class View(host: String, remote_addr: String, http_x_forwarded_for: String, remote_user: String, time_local: String, request: String, status: String, body_bytes_sent: String, request_time: String, upstream_addr: String, upstream_response_time: String, connection: String, connection_requests: String, msec: String, cookie_cookie: String, body_bytes_sent_2: String, http_referer: String, http_user_agent: String, request_body: String)


  def user_track(args: Array[String]): Unit = {
    val Array(source_file, target_file, partitions) = args

    val conf = new SparkConf().setAppName("decrypt log")
    val sc = new SparkContext(conf)
    val file = sc.textFile(source_file).cache().repartition(partitions.toInt)
    sc.newAPIHadoopFile(source_file)
    val job = new Job()
    case class Person(name: String, age: Int)
    file
      .map(l => AESTools.format(l))
      .filter(x => {
        val c = x.split("\001")
        if (c.size > 19) {
          val request = x.split("\001")(5)
          val request_url = request.split(" ")(1)
          !(request_url.endsWith(".js") || request_url.endsWith(".css") || request_url.endsWith(".jpg"))
        } else
          false
      }).saveAsTextFile("", classOf[SnappyCodec])
    //      .saveAsTextFile(target_file)
  }


}
