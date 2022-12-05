package com.ozupesir.bigdata.spark.streaming

import com.ozupesir.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object SparkStreaming11_Req1_BlackList {
  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
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
        val data = kafkaData.value()
        val datas = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    // TODO 周期性获取黑名单数据
    val ds: DStream[((String, String, String), Int)] = adClickData.transform(
      rdd => {
        // TODO 通过JDBC周期性获取黑名单数据
        val blackList = ListBuffer[String]()
        val conn = JDBCUtil.getConnection
        val pstat = conn.prepareStatement("select userid from black_list")

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
            val day = sdf.format(new Date(data.ts.toLong))
            val user = data.user
            val ad = data.ad

            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
      })

    ds.foreachRDD(
      rdd => {
        println(rdd)
        rdd.foreach {
          case ((day, user, ad), count) => {
            println(s"${day} ${user} ${ad} ${count}")
            if (count >= 30) {
              // TODO 如果统计数量超过点击阈值，那么将用户拉入黑名单
              val conn = JDBCUtil.getConnection
              val pstat = conn.prepareStatement(
                """
                  | insert into black_list(userid)
                  | values (?)
                  | on DUPLICATE KEY
                  | UPDATE userid = ?
                  |""".stripMargin)
              pstat.setString(1, user)
              pstat.setString(2, user)
              pstat.executeUpdate()
              pstat.close()
              conn.close()
            } else {
              // TODO 如果没有超过阈值，那么需要将当天的广告点击数量进行更新
              val conn = JDBCUtil.getConnection
              val pstat = conn.prepareStatement(
                """
                  |select
                  | *
                  |from user_ad_count
                  |where dt=? and userid =? and adid=?
                  |""".stripMargin)
              pstat.setString(1, day)
              pstat.setString(2, user)
              pstat.setString(3, ad)
              val rs = pstat.executeQuery()
              // 查询统计表数据
              if (rs.next()) {
                // 如果存在数据，那么更新
                val pstat1: PreparedStatement = conn.prepareStatement(
                  """
                    | update user_ad_count
                    | set count = count + ?
                    | where dt = ? and userid = ? and adid = ?
                    |""".stripMargin)
                pstat1.setInt(1, count)
                pstat1.setString(2, day)
                pstat1.setString(3, user)
                pstat1.setString(4, ad)
                pstat1.executeUpdate()
                pstat1.close()

                // TODO 判断更新后的点击数量是否超过阈值，如果超过，那么将用户拉入黑名单
                val pstat2 = conn.prepareStatement(
                  """
                    | select *
                    | from user_ad_count
                    | where dt=? and userid =? and adid=? and count>=30
                    |""".stripMargin
                )
                pstat2.setString(1, day)
                pstat2.setString(2, user)
                pstat2.setString(3, ad)

                val rs2: ResultSet = pstat2.executeQuery()
                if (rs2.next()) {
                  val pstat3 = conn.prepareStatement(
                    """
                      | insert into black_list(userid) values (?)
                      | on DUPLICATE KEY
                      | UPDATE userid = ?
                      |""".stripMargin)
                  pstat3.setString(1, user)
                  pstat3.setString(2, user)
                  pstat3.executeUpdate()
                  pstat3.close()
                }
                rs2.close()
              } else {
                val pstat1: PreparedStatement = conn.prepareStatement(
                  """
                    | insert into user_ad_count (dt,userid,adid,count)
                    | values(?,?,?,?)
                    |""".stripMargin)
                // 如果不存在数据，就新增
                pstat1.setString(1, day)
                pstat1.setString(2, user)
                pstat1.setString(3, ad)
                pstat1.setInt(4, count)
                pstat1.executeUpdate()
                pstat1.close()
              }
              pstat.close()
              conn.close()
            }
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
