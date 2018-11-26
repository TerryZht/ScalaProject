package com.imooc.spark

import org.apache.spark.sql.{Row, SparkSession}

/**
  * DataFrame 其他操作
  */
object DataFrameCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()
    val rdd = spark.sparkContext.textFile("/tmp/a.txt")
    import spark.implicits._
    val infoDf = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2),line(3))).toDF()
    infoDf.show(30,false)
    infoDf.take(10)
    infoDf.filter("SUBSTR(NAME,0,1)='M'").show
    infoDf.sort(infoDf("NAME")).show
    val infoDf2 = infoDf
    infoDf.join(infoDf2,infoDf("NAME")===infoDf2("NAME")).show()
    
  }
  case class Student(id:Int,name:String,phone:String,email: String)
}
