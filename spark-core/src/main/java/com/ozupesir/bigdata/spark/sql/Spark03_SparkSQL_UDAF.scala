package com.ozupesir.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Spark03_SparkSQL_UDAF {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")

    spark.sql("select age,username from user").show

    spark.udf.register("prefixName", (name: String) => {
      "Name: " + name
    })
    spark.sql("select age,prefixName(username) from user").show

    // TODO 关闭环境
    spark.close()
  }
  /*
  自定义聚合函数类，计算年龄的平均值
  1. 继承UserDefinedAggregateFunction
  2. 重写方法 ctrl+i
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction{
    //  输入数据的结构
    override def inputSchema: StructType = {
      StructType(
        Array(StructField("age",LongType))
      )
    }

    // 缓冲区数据的结构
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType),
        )
      )
    }

    // 函数计算结果的数据类型
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit ={
      buffer(0)=0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

    override def evaluate(buffer: Row): Any = ???
  }
}
