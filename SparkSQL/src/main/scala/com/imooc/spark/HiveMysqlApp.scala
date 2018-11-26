package com.imooc.spark

import org.apache.spark.sql.SparkSession

object HiveMysqlApp {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder().appName("HiveMysqlApp")
      .master("local[2]").getOrCreate()
    //加载hive 数据
    val hiveDf = spark.table("emp")
    //加载mysql数据
    val mysqlDf = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/hive").option("dbtable", "hive.TBLS").option("user", "root").option("password", "root").option("driver", "com.mysql.jdbc.Driver").load()
    //join
    val resultDf = hiveDf.join(mysqlDf,hiveDf.col("empno")===mysqlDf.col("empno"))

    resultDf.select(hiveDf.col("empno"),mysqlDf.col("DNAME"))




    spark.stop()
  }
}
