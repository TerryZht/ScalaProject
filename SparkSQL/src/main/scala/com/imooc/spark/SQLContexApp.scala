package com.imooc.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLContexApp {
  def main(args: Array[String]): Unit = {
//    val path = args(0)
    //创建相应的context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SQLContexApp").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlcontext = new SQLContext(sc)

    // 相应的处理
    val people = sqlcontext.read.format("json").load("file:/E:/people.json")
    people.printSchema()
    people.show()

    //关闭资源
    sc.stop()
  }
}
