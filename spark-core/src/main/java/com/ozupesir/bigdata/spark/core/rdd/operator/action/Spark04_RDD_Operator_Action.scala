package com.ozupesir.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 3, 2, 4))
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3)
    ))

    // TODO - 行动算子
    // count
    val intToLong: collection.Map[Int, Long] = rdd.countByValue()
    println(intToLong)

    val rddCountByKey: collection.Map[String, Long] = rdd1.countByKey()
    println(rddCountByKey)


    sc.stop()
  }
}
