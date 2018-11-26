package com.imooc.log


import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 完成清洗操作
  */
object SparkStatClearJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatClearJob").master("local").getOrCreate()
    val accessRdd = spark.sparkContext.textFile("D:\\Work\\Spark\\Data\\outputs")
//    spark.createDataFrame()
    //Rdd=>dataframe
    val accessDf = spark.createDataFrame(accessRdd.map(x=>AccessConvertUtils.parseLog(x)),AccessConvertUtils.struct)
    val a = spark.createDataFrame(accessRdd.map(x=>AccessConvertUtils.parseLog(x)),AccessConvertUtils.struct)
//    accessRdd.take(10).foreach(println)
    accessDf.printSchema()
    accessDf.show(false)
    //存储 按天进行分区，指定输出个数
//    accessDf.coalesce(1).write.format("parquet").partitionBy("day").save("D:\\Work\\Spark\\Data\\accessDf2")
//    accessDf.write.format("json").save("D:\\Work\\Spark\\Data\\accessDf3.json")
//    accessDf.write.format("parquet").mode("overwrite").save("D:\\Work\\Spark\\Data\\accessDf6")
    a.write.format("parquet").mode("overwrite").save("D:\\Work\\Spark\\Data\\accessDf6")
//      accessDf.write.parquet("D:\\Work\\Spark\\Data\\a")
    spark.stop()
  }
}

