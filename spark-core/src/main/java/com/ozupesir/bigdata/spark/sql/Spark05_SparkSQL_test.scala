package com.ozupesir.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark05_SparkSQL_test {
	def main(args: Array[String]): Unit = {
		// TODO 创建SparkSQL的运行环境
		System.setProperty("HADDOOP_USER_NAME", "root")
		
		val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
		val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
		
		// 使用SparkSQL连接外置的Hive
		// 1. 拷贝Hive-site.xml文件到classpath下
		// 2. 启用Hive的支持
		// 3. 增加对应的依赖关系（包含Mysql驱动）
		
//		spark.sql("use ozupesir").show()
		
		spark.sql("use ozupesir")
		
		spark.sql("drop table if exists user_visit_action")
		spark.sql("drop table if exists product_info")
		spark.sql("drop table if exists city_info")
		
		spark.sql(
			"""
			  | CREATE TABLE `user_visit_action`(
			  |  `date` string,
			  |  `user_id` bigint,
			  |  `session_id` string,
			  |  `page_id` bigint,
			  |  `action_time` string,
			  |  `search_keyword` string,
			  |  `click_category_id` bigint,
			  |  `click_product_id` bigint,
			  |  `order_category_ids` string,
			  |  `order_product_ids` string,
			  |  `pay_category_ids` string,
			  |  `pay_product_ids` string,
			  |  `city_id` bigint)
			  | row format delimited fields terminated by '\t'
			  |""".stripMargin)
		
		spark.sql(
			"""
			  | load data local inpath 'sparkSqlDatas/user_visit_action.txt' into table user_visit_action
			  |""".stripMargin)
		
		spark.sql(
			"""
			  | CREATE TABLE `product_info`(
			  |    `product_id` bigint,
			  |    `product_name` string,
			  |    `extend_info` string)
			  |    row format delimited fields terminated by '\t'
			  |""".stripMargin)
		
		spark.sql(
			"""
			  | load data local inpath 'sparkSqlDatas/product_info.txt' into table product_info
			  |""".stripMargin)
		
		spark.sql(
			"""
			  | CREATE TABLE `city_info`(
			  |  `city_id` bigint,
			  |  `city_name` string,
			  |  `area` string)
			  |  row format delimited fields terminated by '\t'
			  |""".stripMargin)
		
		spark.sql(
			"""
			  | load data local inpath 'sparkSqlDatas/city_info.txt' into table city_info
			  |""".stripMargin)
		
//		spark.sql(
//			"""
//			  | select count(1) from user_visit_action
//			  | union
//			  | select count(1) from product_info
//			  | union
//			  | select count(1) from city_info
//			  | """.stripMargin)
		
		// TODO 关闭环境
		spark.close()
	}
}
