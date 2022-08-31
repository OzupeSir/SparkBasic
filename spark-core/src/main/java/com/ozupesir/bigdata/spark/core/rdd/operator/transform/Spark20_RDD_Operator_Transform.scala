package com.ozupesir.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark20_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - Key-Value类型
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    /*
    reduceByKey：
        combineByKeyWithClassTag[V](
                    (v: V) => v,  // 第一个值不会参与计算
                    func,         // 分区内计算规则
                    func,         // 分区间计算规则
                    partitioner)

    aggregateByKey：
        combineByKeyWithClassTag[U](
                    (v: V) => cleanedSeqOp(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
                    cleanedSeqOp, // 分区内计算规则
                    combOp,       // 分区间计算规则
                    partitioner)

    foldByKey：
        combineByKeyWithClassTag[V](
                    (v: V) => cleanedFunc(createZero(), v),  // 初始值和第一个key的value值进行的分区内数据操作
                    cleanedFunc,  // 分区内计算规则
                    cleanedFunc,  // 分区间计算规则
                    partitioner)

    combineByKey：
        combineByKeyWithClassTag(
                    createCombiner,
                    mergeValue,
                    mergeCombiners,
                    partitioner,
                    mapSideCombine,
                    serializer)(null)

     */

    rdd.reduceByKey(_ + _)
    rdd.aggregateByKey(0)(_ + _, _ + _)
    rdd.foldByKey(0)(_ + _)
    rdd.combineByKey(v => v, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)

    sc.stop()
  }
}
