package com.bbtree.spark.httpclient

object str {

  /**
    * 判断字符串是否是纯数字组成的串，如果是，就返回对应的数值，否则返回0
    * @param str
    * @return
    */
  def strToInt(str: String): Int = {
    val regex = """([0-9]+)""".r
    val res = str match{
      case regex(num) => num
      case _ => "0"
    }
    val resInt = Integer.parseInt(res)
    resInt
  }

  def main(args: Array[String]): Unit = {
    val res = strToInt("012,320,13120")
    println(res)
  }

}
