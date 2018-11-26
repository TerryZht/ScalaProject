package com.imooc.spark

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * DataFrame RDD互操作反射方式
  */
object DataFrameRDDApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()
    inferReflaction(spark)
    spark.close()
  }
  private def programe(spark: SparkSession) = {
    // 编程方式
    val rdd = spark.sparkContext.textFile("/tmp/a.txt")
    val infoRdd = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))
   // val structType = StructType(Array(StructField("id",IntegerType,true),
     // StructField("name",StringType,true)
    //,StructField("age",IntegerType,true)))
    //val infoDf = spark.createDataFrame(infoRdd,structType)
   // infoDf.show()
    //创建临时表
   // infoDf.createTempView("infos")
   // spark.sql("select * from infos where age>30").show()
  }
  private def inferReflaction(spark: SparkSession) = {
    // 反射方式
    val rdd = spark.sparkContext.textFile("/tmp/a.txt")
    // 需要导入隐士转换
    import spark.implicits._
    val infoDf = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()
    infoDf.show()
    //创建临时表
    infoDf.createTempView("infos")
    spark.sql("select * from infos where age>30").show()
  }

  case class Info(id:Int, name:String, age:Int)

}
