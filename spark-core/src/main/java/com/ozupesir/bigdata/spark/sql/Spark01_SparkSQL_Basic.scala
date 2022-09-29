package com.ozupesir.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark01_SparkSQL_Basic {
	def main(args: Array[String]): Unit = {
		// TODO 创建SparkSQL的运行环境
		val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
		
		val spark = SparkSession.builder().config(sparkConf).getOrCreate()
		
		// TODO 执行操作
		// DataFrame
		val df: DataFrame = spark.read.json("datas/user.json")
		//	df.show()
		
		// DataFrame=>SQL
		//	df.createTempView("user")
		//	spark.sql("select * from user").show
		
		// DataFrame => DSL
		// 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
		import spark.implicits._
		df.select("age", "username").show
		df.select($"age" + 1).show
		df.select('age + 1).show
		
		
		// TODO 关闭环境
		spark.close()
	}
}
