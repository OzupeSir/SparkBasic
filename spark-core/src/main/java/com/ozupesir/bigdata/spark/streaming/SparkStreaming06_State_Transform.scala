package com.ozupesir.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Transform {
  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // transform方法可以将底层的RDD获取到后进行操作
    // Code:Driver端
    val newDS: DStream[String] = lines.transform(
      rdd => {
        // Code: Driver端，(周期性执行)
        rdd.map(
          str => {
            // Code: Excutor端
            str
          }
        )
      }

    )

    // Code:Driver端
    val newDS1: DStream[String] = lines.map(
      data => {
        // Code：Excuter端
        data
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }
}
