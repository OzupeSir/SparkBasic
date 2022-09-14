package com.ozupesir.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // reduce ：分区内计算，分区间计算
    //    val i: Int = rdd.reduce(_ + _)

    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )

    println("sum = " + sum)

    // TODO 关闭环境
    sc.stop()
  }
}
