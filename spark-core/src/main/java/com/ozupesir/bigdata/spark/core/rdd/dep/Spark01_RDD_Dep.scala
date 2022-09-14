package com.ozupesir.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile(path = "datas/word.txt")
    println(lines.toDebugString)
    println(lines.dependencies)
    println("*" * 20)

    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println(words.dependencies)
    println("*" * 20)

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    println(wordToOne.toDebugString)
    println(wordToOne.dependencies)
    println("*" * 20)

    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    println(wordToSum.toDebugString)
    println(wordToSum.dependencies)
    println("*" * 20)

    val array: Array[(String, Int)] = wordToSum.collect()

    array.foreach(println)

    sc.stop()
  }
}
