package com.ozupesir.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    // 【1，2】，【3，4】
    val mpRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        // 1,2,3,4
        iter.map(
          num => {
            (index, num)
          }
        )
      }
    )
    mpRDD.collect().foreach(println)
    sc.stop()
  }
}
