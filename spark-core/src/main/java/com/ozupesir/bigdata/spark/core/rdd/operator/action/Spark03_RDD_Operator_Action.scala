package com.ozupesir.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 3, 2, 4))
    val rdd1 = sc.makeRDD(List(1, 3, 2, 4),2)

    // TODO - 行动算子
    // aggregate
    // aggregateByKey：初始值只会参与分区内计算
    // aggregate：初始值也会参与分区内计算，并且和参与分区间计算
    val result: Int = rdd.aggregate(0)(_ + _, _ + _)
    println(result)
    val result01: Int = rdd.aggregate(10)(_ + _, _ + _)
    println(result01)

    val result1: Int = rdd1.aggregate(10)(_ + _, _ + _)
    println("result1",result1)

    // fold
    val foldResult: Int = rdd.fold(0)(_ + _)
    println(foldResult)



    sc.stop()
  }
}
