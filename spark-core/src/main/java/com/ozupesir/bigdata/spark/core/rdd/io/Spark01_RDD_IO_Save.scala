package com.ozupesir.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark01_RDD_IO_Save {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WorlCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(
      List(
        ("a", 1),
        ("b", 2),
        ("c", 3)
      )
    )

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    sc.stop()

  }
}