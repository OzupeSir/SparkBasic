package com.ozupesir.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Spark05_SparkSQL_test2 {
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

    spark.udf.register("StrDeal", functions.udaf(new MyStrUDAF()))

    spark.sql("use ozupesir")

    spark.sql(
      """
			  |select c.area,c.city_name,p.product_name,count(1) as click_count
			  |from user_visit_action u
			  |join product_info p on u.click_product_id=p.product_id
			  |join city_info c on u.city_id=c.city_id
			  |GROUP by c.area,p.product_name,c.city_name
			  |""".stripMargin).createTempView("src_tbl")


    spark.sql(
      """
			  | select
			  |	 area,product_name,sum(click_count) as click_count,
        |  StrDeal(click_count,city_name) as mark
			  | from src_tbl
			  | group by area,product_name
			  |""".stripMargin).createTempView("area_count")

    spark.sql(
      """
			  | select
			  |	 area,
			  |  product_name,
			  |  click_count,
        |  mark,
			  |  rank() over(partition by area order by click_count desc) as rank
			  | from area_count
			  |""".stripMargin).createTempView("area_count_rank")

    spark.sql(
      """
			  | select
			  |	 area,
			  |  product_name,
			  |  click_count,
        |  mark,
			  |  rank
			  | from area_count_rank where rank<=3
			  |""".stripMargin).show(50,false)

    // TODO 关闭环境
    spark.close()
  }

  case class Info(var click_count: Long, var city_name: String)

  case class Buff(var cityMap: mutable.Map[String, Long])

  class MyStrUDAF() extends Aggregator[Info, Buff, String] {
    override def zero: Buff = Buff(mutable.Map[String, Long]())

    override def reduce(b: Buff, a: Info): Buff = {
      val click_count = b.cityMap.getOrElse(a.city_name, 0L) + a.click_count
      b.cityMap.update(a.city_name, click_count)
      b
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.cityMap = b1.cityMap.foldLeft(b2.cityMap) {
        case (map, (city, cnt)) => {
          val new_count = map.getOrElse(city, 0L) + cnt
          map.update(city, new_count)
          map
        }
      }
      b1
    }

    override def finish(reduction: Buff): String = {
      val remarkList = ListBuffer[String]()

      val totalcnt = reduction.cityMap.map(_._2).sum

      val cityCntList: List[(String, Long)] = reduction.cityMap.toList.sortBy(-_._2).take(2)
      val hasMore = reduction.cityMap.size > 2
      var totalBuff = 0L

      cityCntList.foreach {
        case (city, cnt) => {
          val r = (cnt * 100D / totalcnt).formatted("%.2f")
          totalBuff += cnt
          remarkList.append(s"${city}:${r}% ")
        }
      }
      if (hasMore) {
        val r = ((totalcnt - totalBuff) * 100D / totalcnt).formatted("%.2f")
        remarkList.append(s"其他 ${r}%")
      }
      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
