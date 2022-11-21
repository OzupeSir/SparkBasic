package com.ozupesir.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark05_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 使用SparkSQL连接外置的Hive
    // 1. 拷贝Hive-site.xml文件到classpath下
    // 2. 启用Hive的支持
    // 3. 增加对应的依赖关系（包含Mysql驱动）
   
   spark.sql("show databases").show

    // TODO 关闭环境
    spark.close()
  }
}
