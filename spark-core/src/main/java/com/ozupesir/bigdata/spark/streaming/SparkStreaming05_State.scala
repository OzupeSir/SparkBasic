package com.ozupesir.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_State {
  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //使用有状态路径时，需要设定checkpoint
    ssc.checkpoint("cp")

    // 无状态数据操作，只对当前的采集周内的数据进行处理
    // 在某些场合下，需要保留数据统计结果（状态），实现数据的汇总

    val datas: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne = datas.map((_, 1))

    //    val wordToCount = wordToOne.reduceByKey(_+_)

    // updateStateByKey: 根据key对数据的状态进行更新
    // 传递的参数中含有两个值
    // 第一个值标识相同的key的value数据
    // 第二个值标识缓存区相同的value值
    val state = wordToOne.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
