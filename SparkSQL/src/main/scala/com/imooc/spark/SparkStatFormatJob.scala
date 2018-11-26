package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * 第一步：抽取所需数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local").getOrCreate()
    val access = spark.sparkContext.textFile("D:\\Work\\Spark\\Data\\accessNew.log")
//    access.take(10).foreach(println)
    access.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3)+ " " + splits(4)
      val url = splits(11).replaceAll("\"","")
      val traffic = splits(9)
      DateUtils.parse(time) +"\t" +url +"\t" + traffic +"\t"+ip
    }
    ).saveAsTextFile("D:\\Work\\Spark\\Data\\outputs")

    spark.stop()
  }

}
