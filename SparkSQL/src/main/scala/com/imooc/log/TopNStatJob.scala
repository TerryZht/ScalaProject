package com.imooc.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
object TopNStatJob {

  def vedioAccessTopNStat(spark: SparkSession, accessDf: DataFrame) :Unit= {
    //    import spark.implicits._
    //    val res = accessDf.filter($"day" === "20161110" && $"cmsType" === "video").groupBy("day","cmsId")
    //          .agg(count("cmsId").as("times")).orderBy($"times".desc)
    //    res.show(false)
    accessDf.createTempView("access_logs")
    val res = spark.sql("select day,cmsId,count(1) as times from access_logs where day='20161110' and " +
      "cmsType='video'" +
      "group by day,cmsId order by times desc")
    try {
      res.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAcessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId").toString
          val times = info.getAs[Long]("times")
          list.append(DayVideoAcessStat(day, cmsId, times))
        })
        StatDAO.insertVideoAccessTopN(list)
      })
    } catch{
      case e:Exception=>e.printStackTrace()
    }
  }


  def cityAccessTopNStat(spark: SparkSession, accessDf: DataFrame) = {
        import spark.implicits._
        val cityAccessTopN = accessDf.filter($"day" === "20161110" && $"cmsType" === "video").
          groupBy("day","city","cmsId").agg(count("cmsId").as("times"))

        val res =   cityAccessTopN.select(cityAccessTopN("day"),
            cityAccessTopN("city"),
            cityAccessTopN("cmsId"),
            cityAccessTopN("times"),
            row_number().over(
              Window.partitionBy(cityAccessTopN("city"))
                .orderBy( cityAccessTopN("times").desc)
            ).as("times_rank")).filter("times_rank<=3")
    try {
      res.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayCityAccessStat]
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Integer]("cmsId").toLong
          val times = info.getAs[Long]("times")
          val city = info.getAs[String]("city")
          val rank = info.getAs[Integer]("times_rank").toLong
          list.append(DayCityAccessStat(day, cmsId, city,times,rank))
        })
        StatDAO.insertCityAccessTopN(list)
      })
    } catch{
      case e:Exception=>e.printStackTrace()
    }
  }

  def videoTrafficTopNStat(spark: SparkSession, accessDf: DataFrame) = {
    import spark.implicits._
    val trafficAccessTopN = accessDf.filter($"day" === "20161110" && $"cmsType" === "video").
      groupBy("day","city","cmsId").agg(count("cmsId").as("times"))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob").master("local").getOrCreate()
//    val accessDf = spark.read.format("parquet").load("D:\\Work\\Spark\\Data\\accessDf2")
    val accessRdd = spark.sparkContext.textFile("D:\\Work\\Spark\\Data\\outputs")
    val accessDf = spark.createDataFrame(accessRdd.map(x=>AccessConvertUtils.parseLog(x)),AccessConvertUtils.struct)

//    accessDf.printSchema()
//    accessDf.show()
//    val day = "20161110"
//      StatDAO.deleteData(day)
    vedioAccessTopNStat(spark,accessDf)
//    cityAccessTopNStat(spark,accessDf)
    //按照流量进行统计
//    videoTrafficTopNStat(spark,accessDf)
    spark.stop()
  }

}
