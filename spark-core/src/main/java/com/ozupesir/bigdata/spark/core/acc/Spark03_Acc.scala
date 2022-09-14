package com.ozupesir.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    // 获取系统累加器
    // Spark默认酒系统了简单数据的累加器

    val sumAcc: LongAccumulator = sc.longAccumulator("sum")

    //    sc.doubleAccumulator()
    //    sc.collectionAccumulator()

    val mapRDD: RDD[Int] = rdd.map(
      num => {
        // 使用累加器
        sumAcc.add(num)
        num
      }
    )

    // 获取累加器的值
    // 少加：转换算子中调用累加器，如果没有行动算子，那么就不会执行
    // 多加：如果会有多个算子，就会执行多次
    // 一般情况下，累加器会放置在行动算子中进行操作
    mapRDD.collect()
    mapRDD.collect()

    println("sum = " + sumAcc.value)

    // TODO 关闭环境
    sc.stop()
  }
}
