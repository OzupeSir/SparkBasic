package com.ozupesir.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object Spark03_SparkSQL_UDAF1 {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")


    spark.sql("select age,username from user").show
    spark.sql("select age,username from json.`datas/user.json`").show

    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))
    spark.sql("select ageAvg(age) from user").show

    // TODO 关闭环境
    spark.close()
  }

  /*
  自定义聚合函数类，计算年龄的平均值
  1. 继承 org.apache.spark.sql.expressions.Aggregator
    IN: 输入的数据类型 Long
    BUF: 缓冲区的数据 Buff
    OUT: 输出的数据类型 Long
  2. 重写方法 ctrl+i(6种方法)
   */
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDAF() extends Aggregator[Long, Buff, Long] {

    // z & zero ：初始值或0值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入的数据更新缓冲区的数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.total = buff.total + in
      buff.count += 1
      buff
    }

    // 合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    // 计算结果
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    // 缓冲区比的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 缓冲区比的解码 操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}
