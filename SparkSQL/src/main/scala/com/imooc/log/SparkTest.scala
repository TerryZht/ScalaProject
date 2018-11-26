package com.imooc.log

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object SparkTest {
  def main(args: Array[String]): Unit = {
//    val url = "-"
//    val domain = "http://www.imooc.com/"
//
//    if (url.length > 10) {
//      val cmsTypeId = url.substring(url.indexOf(domain) + domain.length).split("/")
//      val c1 = cmsTypeId(0)
//      val c2 = cmsTypeId(1).toLong
//      println(c1)
//      println(c2)
//    }
    //    println(cmsTypeId.length)
//val spark = SparkSession.builder().appName("SparkStatClearJob").master("local").getOrCreate()
//val conf = new SparkConf().setAppName("SparkTest").setMaster("local")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//    val sqlcontext = new SQLContext(sc)
//    val idAgeRDDRow = sc.parallelize(Array(Row(1, 30), Row(2, 29), Row(4, 21)))
//    val schema = StructType(Array(StructField("id", DataTypes.IntegerType), StructField("age", DataTypes.IntegerType)))
//    val idAgeDF = sqlcontext.createDataFrame(idAgeRDDRow, schema)
//    idAgeDF.show(false)
//    idAgeDF.write.format("parquet").mode("overwrite").save("D:\\Work\\Spark\\Data\\accessDf6")
      val a= "3669?from=itblog"
    val p = """\?""".r
    println(a.contains("?"))
    println(a.split("[?]")(0).toInt)
    val c="pls"
    val p1 = """\d+""".r
    println((p1 findAllIn c toList ) isEmpty)



  }
}
