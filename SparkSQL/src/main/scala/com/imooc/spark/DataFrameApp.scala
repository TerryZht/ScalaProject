package com.imooc.spark

import org.apache.spark.sql.SparkSession

object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()
    // 将json文件加载成dataframe
    val pepoleDf = spark.read.format("json").load("/tmp/a.json")
    pepoleDf.printSchema()
    //输出前20条记录
    pepoleDf.show()
    //查询某列所有数据
    pepoleDf.select("name").show()
    //查询某几列所有的数据，并对列进行计算
    pepoleDf.select(pepoleDf.col("name"),(pepoleDf.col("age")+10) as ("age2")).show()
    //过滤数据
    pepoleDf.filter(pepoleDf.col("age")>19).show()
    //根据某一列进行分组，然后进行聚合
    pepoleDf.groupBy("age").count()
    pepoleDf.select("")
    spark.stop()

  }
}
