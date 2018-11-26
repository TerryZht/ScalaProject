package com.imooc.spark

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

/**
  * 日期时间解析工具类
  */
object DateUtils {
  /**
    * 输入文件日期格式
    * SimpleDateFormat 是线程不安全的,使用FastDateFormat
    */
//  val  YYMMDDHHMM_TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
  val  YYMMDDHHMM_TIME_FORMAT = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
  //输出格式
  val TARGET_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  /**
    * 获取时间
    * @parm
    */
  def parse(time:String)={
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 获取输入时间转成long类型
    */
  def getTime(time: String)={
    try {
      YYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    } catch {
      case e :Exception=>{
        0l
      }
    }

  }

  def main(args: Array[String]): Unit = {
    println(parse("[10/Nov/2016:00:01:12 +0800]"))
  }
}
