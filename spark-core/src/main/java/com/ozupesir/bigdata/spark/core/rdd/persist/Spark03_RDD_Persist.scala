package com.ozupesir.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(
      word => {
        println("@@@@@@@@@@@@@@@@@@@")
        (word, 1)
      }
    )
    // cache默认持久化的操作，只能将数据保存到内存中，如果想要保存到内存中需要使用persist
    mapRDD.cache()

    // 持久化操作必须是在行动算子执行时才会触发完成
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)
    println("*" * 20)


    val reduceRDD1: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    reduceRDD1.collect().foreach(println)

    sc.stop()

  }
}
