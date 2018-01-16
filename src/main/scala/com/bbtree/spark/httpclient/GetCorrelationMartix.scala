//package com.bbtree.spark.httpclient
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//object GetCorrelationMartix {
//  var myTrainCsvPath: String = "yourpath"
//  var secondTrainCsvPath: String = "yourpath"
//
//  /**
//    * 使用DataFrameStatFunctions中的API
//    * TODO：参考mlib中的correlationExample.scala，修改为矩阵运算
//    */
//  def correlationAndCleanAgain_w(columnNames: Array[String], dataFrame: DataFrame): DataFrame = {
//    // 计算一个n X n的相关系数矩阵【我只计算了对角线的一侧】，并将结果保存在Double二维数组中
//    val storeCorrsMatrix = Array.ofDim[Double](columnNames.length, columnNames.length) // 使用ofDim创建二维数组
//    var tempDf: DataFrame = dataFrame
//    for (i <- 0 until columnNames.length - 1) { // 我们仅计算矩阵对角线一侧就行
//      //      println(f"${columnNames(i)}%5s 与其他列-------------------------------------------") // 这里的f插值没有起作用
//      for (j <- i + 1 until columnNames.length) {
//        var corrValue: Double = dataFrame.stat.corr(columnNames(i), columnNames(j))
//        if (corrValue < 0)
//          corrValue = corrValue.abs // 处理为绝对值
//        else if (corrValue.isNaN)
//          corrValue = 0 // NaN处理为0
//        /*        if (corrValue > 0.8)
//                  println(s"${columnNames(i)}与${columnNames(j)}的相关系数值为：$corrValue") // s字符串插值器，scala2.10添加*/
//        storeCorrsMatrix(i)(j) = corrValue
//      }
//    }
//    println("它应该是0：" + storeCorrsMatrix(5)(5) + " ；它应该是0.9645:" + storeCorrsMatrix(22)(23)) // 简单验证
//
//    val corrsMatrixClong: Array[Double] = storeCorrsMatrix.flatten.sortWith(_ > _) // 二维数组按行拼接为一维数组,然后从小到大排序
//    //    storeCorrsMatrix.copyToArray(corrsMatrixClong, 0)
//    /*println("storeCorrs数组的长度为：" + storeCorrs.length) // 276(23+22+21+...1)
//    println("front_track与rear_track的相关系数值为：" + storeCorrs(storeCorrs.length-1))*/
//    //    println(corrsMatrixClong.mkString("  "))
//    println(corrsMatrixClong.length) // 576=24*24
//    // 每次取复制的数组中的第一个值（前提是大于阈值0.8）；在二维数组中找到对应的两个变量的位置记为valueX和valueY
//    // 为了简便，我取的下标，因为我这里就8个大于阈值0.8的数
//    for (i <- 0 to 7) {
//      val indexArray: Array[Int] = findIndexByValue_w(storeCorrsMatrix, corrsMatrixClong(i))
//      val indexX = indexArray(0)
//      val indexY = indexArray(1)
//      var sumX: Double = 0.0
//      var sumY: Double = 0.0
//
//      // 计算sumX
//      for (m <- 0 to indexX) { // 为避免indexX=0，这里没使用until（因为对角线上的值都为0，让它加吧）
//        sumX += storeCorrsMatrix(m)(indexX)
//      }
//      if (indexX != 23) {
//        for (n <- indexX + 1 to 23) {
//          sumX += storeCorrsMatrix(indexX)(n)
//        }
//      }
//
//      // 计算sumY
//      for (m <- 0 until indexY) {
//        sumY += storeCorrsMatrix(m)(indexY)
//      }
//      if (indexY != 23) {
//        for (n <- indexY + 1 to 23) {
//          sumY += storeCorrsMatrix(indexY)(n)
//        }
//      }
//
//      // 删除sumX和sumY中较大的对应的变量（indexX或indexY对应的变量）
//      if (sumX > sumY) {
//        tempDf = tempDf.drop(columnNames(indexX))
//        println(s"删除掉： ${columnNames(indexX)}")
//      }
//      else {
//        tempDf = tempDf.drop(columnNames(indexY))
//        println(s"删除掉： ${columnNames(indexY)}")
//      }
//    }
//    // 返回清理完选择出的变量之后的DataFrame
//    tempDf
//  }
//
//  /**
//    * 在原相关系数矩阵中找给定的相关系数值的对应的2个变量
//    * return: 返回二维数组的下标（就是对应的2个变量的位置）
//    * TODO：corr值不唯一怎么办？极小概率事件吧？
//    */
//  def findIndexByValue_w(storeCorrsMatrix: Array[Array[Double]], corrValue: Double): Array[Int] = {
//    val corrIndex: Array[Int] = new Array[Int](2)
//    var flag: Boolean = true
//    for (i <- 0 to 22) {
//      for (j <- i + 1 to 23 if flag) {
//        if (storeCorrsMatrix(i)(j) == corrValue) { // Scala没有三目运算符，对break跳出循环的支持也不太“优美”(抛出异常)
//          corrIndex(0) = i
//          corrIndex(1) = j
//          println(s"$corrValue index_i is $i ,index_j is $j")
//          flag = false
//        }
//      }
//    }
//    corrIndex
//  }
//
//  /**
//    * 计算相关系数的主要函数
//    */
//  def correlationForEach_w(csvPath: String, spark: SparkSession): Unit = {
//    // 获取数据
//    val rawTrainDf: DataFrame = spark.read.format("csv").option("header", true).option("inferSchema", true).load(myTrainCsvPath)
//    //    rawTrainDf.printSchema()
//    //    rawTrainDf.show()
//    // 设置哪些是特征，哪个是标签，但是还没有开始转换
//    /*    val assembler = new VectorAssembler()
//          .setInputCols(Array(""))
//          .setOutputCol("features")
//        // 开始转换
//        val output = assembler.transform(rawTrainDf)
//        output.select("features", "sale_quantity").show*/
//
//    val columnNames: Array[String] = rawTrainDf.columns
//    //    计算相关系数
//    val cleanAgainDf = correlationAndCleanAgain_w(columnNames, rawTrainDf)
//    rawTrainDf.printSchema()
//    println("======================================================================")
//    cleanAgainDf.printSchema()
//    cleanAgainDf.coalesce(1)
//      .write
//      .mode("overwrite")
//      .option("header", true)
//      .format("csv")
//      .save(secondTrainCsvPath)
//    println("训练集保存完毕，保存路径————————：" + secondTrainCsvPath)
//    spark.close()
//  }
//
//  def main(args: Array[String]): Unit = {
//    // 避免大量INFO信息干扰，必须设置在开头
//    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//
//    val spark = SparkSession.builder()
//      .master("local")
//      .appName("SomeStatistics")
//      .config("spark.some.config.option", "some-value") // TODO:对这个参数的设置的理解还不到位
//      .getOrCreate()
//
//    correlationForEach_w(myTrainCsvPath, spark)
//  }
//
//}
