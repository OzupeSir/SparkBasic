package com.ozupesir.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val user: User = new User()
    // Task not serializable
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )


    val user1: User1 = new User1()
    rdd.foreach(
      num => {
        println("age = " + (user1.age + num))
      }
    )

    sc.stop()
  }

  class User {
    var age: Int = 30
  }

  // class user extends Serializable
  // 样例类在编译时，会自动混入序列化特质（实现可序列化接口）

  case class User1() {
    var age: Int = 30
  }
  // RDD算子中传递的函数是会包含闭包操作，name酒会进行检测功能
  // 闭包检测
}
