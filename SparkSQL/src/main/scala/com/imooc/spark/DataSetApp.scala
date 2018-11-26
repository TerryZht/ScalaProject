//package com.imooc.spark
//
//import org.apache.spark.sql.SparkSession
//
///**
//  * DataSet 操作
//  */
//object DataSetApp {
//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("DataSetApp").master("local[2]").getOrCreate()
//    //spark 如何解析csv文件？
//    val path = "/tmp/a.csv"
//    val df = spark.read.option("header","true").option("interSchema","true").csv(path = path)
//    df.show()
//    val ds = df.as[Sales]
//    ds.map(line=>line.itemId).show
//
//
//  }
//
//  //case class Sales(transaction:Int,customerId:Int,itemId:Int,amountPaid:True)
//
//}
