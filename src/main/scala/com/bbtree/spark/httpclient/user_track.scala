package com.bbtree.spark.httpclient

import java.text.{SimpleDateFormat, DateFormat}
import java.util.Locale

import org.apache.spark.{SparkConf, SparkContext}


/*
* Created by liufei on 18 / 1 / 13.
*/


object user_track {


  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: userTrack <source_file> <partitions> <target_file> <duration>")
      System.exit(1)
    }
    user_track(args)

  }

  //  case class View(time: String, uri: String, uid: String)

  case class View(user_id: String, time: String, url: String, url_name: String, host: String, method: String, status: String, parameter: String)


  def user_track(args: Array[String]): Unit = {
    val Array(source_file, partitions, target_file, duration) = args

    val conf = new SparkConf().setAppName("user track")
    val sc = new SparkContext(conf)
    val file = sc.textFile(source_file).cache().repartition(partitions.toInt)
    //    val first_uri = "/service/v2/circle/main_user/loginEnd"

    val group_file = file.map(x => {
      val arr = x.split("\001")
      View(arr(0), arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7))
    }
    ).filter(v => {
      !"".equals(v.time)
    })
      .sortBy(v => v.user_id + v.time)
      .groupBy(v => v.user_id).cache()

    group_file.flatMap(x => {
      var i = 0
      var j = 0

      var tmp: View = View("", "0000-00-01 00:00:00", "", "", "", "", "", "")
      x._2.map(v => {
        if ("0000-00-01 00:00:00".equals(tmp.time)) {
          tmp = v
          i = 1
          j = 1
        } else {
          i = i + 1
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          //          val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US)
          val pre = dateFormat.parse(tmp.time).getTime
          val suffix = dateFormat.parse(v.time).getTime
          if (suffix - pre > duration.toInt * 60 * 1000) {
            j = j + 1
            i = 1
          }
          tmp = v
        }
        //        v.user_id + "\t" + v.time + "\t" + v.uri + "\t" + v.uri_name + "\t" + i + "\t" + j
        v.user_id + "\001" + v.time + "\001" + v.url + "\001" + v.url_name + "\001" + v.host + "\001" + v.method + "\001" + v.status + "\001" + v.parameter + "\001" + i + "\001" + j
      })
    }
    ).saveAsTextFile(target_file)
  }

//  def user_track_2(args: Array[String]): Unit = {
//    val Array(source_file, partitions, target_file) = args
//
//    val conf = new SparkConf().setAppName("user track")
//    //    System.setProperty("spark.serializer", "spark.KryoSerializer")
//    val sc = new SparkContext(conf)
//    val file = sc.textFile(source_file).cache().repartition(partitions.toInt)
//    val first_uri = "/service/v2/circle/main_user/loginEnd"
//
//
//    val group_file = file.map(x => {
//      val arr = x.split("\001")
//      View(arr(0), arr(1), arr(2), arr(3), arr(4))
//    }
//    ).sortBy(v => v.uid + v.time)
//      .groupBy(v => v.uid).cache()
//
//    group_file.flatMap(x => {
//      var i = 0
//      var j = 0
//      x._2.map(v => {
//        if (i == 0) {
//          i = i + 1
//          j = j + 1
//        } else if (v.uri.contains(first_uri)) {
//          i = 1
//          j = j + 1
//        } else {
//          i = i + 1
//        }
//        v.uid + "\t" + v.time + "\t" + v.uri + "\t" + v.uri_name + "\t" + v.id + "\t" + i + "\t" + j
//
//      })
//    }).saveAsTextFile(target_file)
//  }


}
