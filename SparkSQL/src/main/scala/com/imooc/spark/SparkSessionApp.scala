package com.imooc.spark

import org.apache.spark.sql.SparkSession

class SparkSessionApp {
  def main(args:Array[String]): Unit ={
    val spark = SparkSession.builder().appName("SparkSessionApp").getOrCreate()
    val pepole = spark.read.json("D:\\Work\\Scala\\people.json")
    pepole.show()
    spark.close()
  }
}
