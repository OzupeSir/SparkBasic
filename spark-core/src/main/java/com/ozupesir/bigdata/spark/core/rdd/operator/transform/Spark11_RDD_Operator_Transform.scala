package com.ozupesir.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark11_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - coalesce(扩大或修改分区)
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    // coalesce 默认不会让将分区的数据打乱重新组合
    // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    // 如果想要让数据均衡，可以进行shuffle处理
    //    val newRDD: RDD[Int] = rdd.coalesce(2,)

    // coalesce算子可以扩分区，但是如果不进行shuffle操作，是没有意义的，不起作用。
    // 所以如果想要实现扩大分区的效果，需要使用shuffle操作

    // spark提供了一个简化操作
    // 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
    // 扩大分区：repartition，底层代码调用的就是coalesce，而且必定shuffle
    // val newRDD: RDD[Int] = rdd.coalesce(3, true)
    val newRDD: RDD[Int] = rdd.repartition(3)

    newRDD.saveAsTextFile("output")

    sc.stop()

  }
}
