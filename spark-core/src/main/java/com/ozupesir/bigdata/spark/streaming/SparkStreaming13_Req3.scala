package com.ozupesir.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer


object SparkStreaming13_Req3 {
  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(5))

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
    val reduceDS: DStream[(Long, Int)] = adClickData
      .filter(_.user.contains("1"))
      .map(
        data => {
          val ts: Long = data.ts.toLong
          val newTS: Long = ts / 10000 * 10000
          (newTS, 1)
        }
      ).reduceByKeyAndWindow(_ + _, Seconds(60), Seconds(10))
    // 最近一份份那种，每10秒计算一次
    // 12:01=>12:00
    //    adClickData.print()
    //    adClickData
    //      .filter(_.user.contains("1")).print()
    //    adClickData.filter(data => {
    //      data.user.contains("1")
    //    }).print()
    // 这里涉及窗口滑动的概念

    reduceDS.foreachRDD(
      rdd => {
        val list: ListBuffer[String] = ListBuffer[String]()

        val datas: Array[(Long, Int)] = rdd.sortByKey(ascending = true).collect()

        // 输出文件
        val out = new PrintWriter(new FileWriter("sparkSqlDatas/adclick/adclick.json"))
        datas.foreach {
          case (time, cnt) =>
            val timeString: String = new SimpleDateFormat("mm:ss").format(new Date(time))
            list.append(
              s"""
                 |{ "xtime":"$timeString", "yval":"$cnt" }
                 |""".stripMargin)

        }
        out.println("[" + list.mkString(",") + "]")
        out.flush()
        out.close()
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }

  // 广告点击数据
  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
