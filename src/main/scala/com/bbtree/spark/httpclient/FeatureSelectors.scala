//package com.bbtree.spark.httpclient
//import java.util
//
//import org.apache.spark.mllib.linalg.{Vector, Vectors}
//import org.apache.spark.mllib.linalg.distributed.RowMatrix
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, SQLContext, Row}
//import org.apache.spark.{SparkContext, SparkConf}
//import java.util.Arrays
//
//import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
//import org.apache.spark.ml.feature.VectorSlicer
//import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
//object FeatureSelectors {
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("test").setMaster("local")
//    val sc = new SparkContext(conf)
//    val sql = new SQLContext(sc)
//
//    val data = Arrays.asList(
//      //            Row(Vectors.dense(-2.0, 2, 0.0)),
//      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3)))),
//      Row(Vectors.dense(-2.0, 2, 0.0))
//    )
//
//    val defaultAttr: NumericAttribute = NumericAttribute.defaultAttr
//    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
//    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])
//    //从三列中选择两列参与模型训练
//    val dataset = sql.createDataFrame(data, StructType(Array(attrGroup.toStructField())))
//
//    dataset.printSchema()
//
//    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
//    //setIndices(Array(1)) 第二列   setNames(Array("f3")) 第三列
//    //        slicer.setIndices(Array(1)).setNames(Array("f3"))
//    slicer.setIndices(Array(1)).setNames(Array("f3"))
//    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))
//
//    val output: DataFrame = slicer.transform(dataset)
//    output.printSchema()
//    output.show(false)
//
//    output.select("features").show()
//
//    //        val out: RDD[Row] = output.rdd.map(row => Row(row.get(0),row.get(1)))
//    val out: DataFrame = output.select("features")
//
//
//    val rdd: RDD[Row] = out.toDF().map{ row =>
//      val r: Vector = row.getAs[Vector](0)
//      Row(r.apply(0),r.apply(1))
//      //        println("---"+r.apply(0)+"---"+r.apply(1))
//    }
//    val fields = new util.ArrayList[StructField];
//    fields.add(DataTypes.createStructField("id", DataTypes.DoubleType, true));
//    fields.add(DataTypes.createStructField("feature", DataTypes.DoubleType, true));
//    val structType = DataTypes.createStructType(fields);
//    sql.createDataFrame(rdd,structType).show()
//
//  }
//
//}
