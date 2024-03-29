package com.ozupesir.bigdata.spark.streaming

import com.ozupesir.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object SparkStreaming11_Req1_BlackList1 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "master:9092,slave1:9092,slave2:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "ozupesir",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("ozupesir"), kafkaPara)
    )

    val adClickData: DStream[AdClickData] = kafkaDataDS.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    // TODO 周期性获取黑名单数据
    val ds: DStream[((String, String, String), Int)] = adClickData.transform(
      rdd => {
        val blackList: ListBuffer[String] = ListBuffer[String]()
        // TODO 通过JDBC周期性获取黑名单数据
        val conn: Connection = JDBCUtil.getConnection
        val pstat: PreparedStatement = conn.prepareStatement("select userid from black_list")

        val rs: ResultSet = pstat.executeQuery()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }

        rs.close()
        pstat.close()
        conn.close()

        // TODO 判断点击用户是否在黑名单仲
        val filterRDD: RDD[AdClickData] = rdd.filter(
          data => {
            !blackList.contains(data.user)
          }
        )

        // TODO 如果用户不在黑名单仲，那么进行统计数量（每个采集周期的）
        filterRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day: String = sdf.format(new Date(data.ts.toLong))
            val user: String = data.user
            val ad: String = data.ad

            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
      })

    ds.foreachRDD(
      rdd => {
        // rdd.foreach 方法会每一条数据创建连接
        // foreach方法是RDD的算子，算子之外的代码是在Driver端执行的，算子内的代码是在Executor端执行
        // 这样就会涉及闭包操作，Driver端的数据就需要传递到Executor端，需要将数据进行序列化
        // 数据库的连接对象是不能序列化的

        // RDD提供了一个算子可以有效提供效率:ForeachPartition
//        rdd.foreachPartition(
//          iter => {
//            val conn = JDBCUtil.getConnection
//            iter.foreach {
//              case ((day, user, ad), count) => {
//
//              }
//            }
//            conn.close()
//          }
//        )

        rdd.foreach {
          case ((day, user, ad), count) =>
            println(s"$day $user $ad $count")
            if (count >= 30) {
              // TODO 如果统计数量超过点击阈值，那么将用户拉入黑名单
              val conn: Connection = JDBCUtil.getConnection
              val sql: String =
                """
                  | insert into black_list(userid)
                  | values (?)
                  | on DUPLICATE KEY
                  | UPDATE userid = ?
                  |""".stripMargin
              JDBCUtil.executeUpdate(conn, sql, Array(user, user))
              conn.close()
            } else {
              val conn: Connection = JDBCUtil.getConnection
              // TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新
              val sql: String =
                """
                  |select
                  | *
                  |from user_ad_count
                  |where dt=? and userid =? and adid=?
                  |""".stripMargin
              val flg: Boolean = JDBCUtil.ifExist(conn, sql, Array(day, user, ad))
              // 查询统计表数据
              if (flg) {
                // 如果存在数据，那么更新
                val sql: String =
                  """
                    | update user_ad_count
                    | set count = count + ?
                    | where dt = ? and userid = ? and adid = ?
                    |""".stripMargin
                JDBCUtil.executeUpdate(conn, sql, Array(count, day, user, ad))

                // TODO 判断更新后的点击数量是否超过阈值，如果超过，那么将用户拉入黑名单
                val sql2: String =
                  """
                   select *
                   from user_ad_count
                   where dt=? and userid =? and adid=? and count>=30
                  """.stripMargin
                val flg1: Boolean = JDBCUtil.ifExist(conn, sql2, Array(day, user, ad))

                if (flg1) {
                  val sql: String =
                    """
                      | insert into black_list(userid)
                      | values (?)
                      | on DUPLICATE KEY
                      | UPDATE userid = ?
                      |""".stripMargin
                  JDBCUtil.executeUpdate(conn, sql, Array(user, user))
                }
              } else {
                // 如果不存在数据，就新增
                val sql: String =
                  """
                    | insert into user_ad_count (dt,userid,adid,count)
                    | values(?,?,?,?)
                    |""".stripMargin
                JDBCUtil.executeUpdate(conn, sql, Array(day, user, ad, count))
              }
              conn.close()
            }

        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  // 广告点击数据
  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
