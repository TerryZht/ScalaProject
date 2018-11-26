package com.imooc.spark

import org.apache.spark.sql.SparkSession

object ParquetApp {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("ParquetApp")
      .master("local[2]").getOrCreate()

    val userDf = spark.read.format("parquet").load("/tmp/a.parquet")
    userDf.printSchema()
    userDf.show()
    userDf.select("name","favarate_color").show()
    userDf.select("name","favarate_color").write.format("json").save("/tmp/a.json")

    //处理parquet数据可以使用如下操作，没有指定format 默认parquet格式
    spark.read.load("/tmp/a.parquet").show()
    //保存数据到hive 中默认混淆分区的数值是200
    //spark.sparkContext.setConf("spark.sql.shuffle.partitions","10")

    spark.stop()
  }
}
