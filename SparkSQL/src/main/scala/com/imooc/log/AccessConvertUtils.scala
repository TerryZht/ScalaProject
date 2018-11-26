package com.imooc.log

import com.sun.prism.PixelFormat.DataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * 输入转换为输出
  */
object AccessConvertUtils {
//  val struct = StructType(
//    //定义输出字段
//    Array(
//      StructField("url",DataTypes.StringType,true),
//      StructField("cmsType",DataTypes.StringType,true),
//      StructField("cmsId",DataTypes.StringType,true),
//      StructField("traffic",DataTypes.StringType,true),
//      StructField("ip",DataTypes.StringType,true),
//      StructField("city",DataTypes.StringType,true),
//      StructField("time",DataTypes.StringType,true),
//      StructField("day",DataTypes.StringType,true)
//    )
//  )
  val struct = new StructType().add("url",StringType,true).add("cmsType",StringType,true).add("cmsId",IntegerType,true)
    .add("traffic",IntegerType,true).add("ip",StringType,true).add("city",StringType,true).add("time",StringType,true)
    .add("day",StringType,true)
  /**
    * 根据输入转换成输出的样式
    * @param log
    */
  def parseLog(log:String)={
    try{
      val splits = log.split("\t")
      val url = splits(1)
      val traffic = splits(2).toInt
      val ip = splits(3)
      val domain = "http://www.imooc.com/"
      var cmsType = ""
      var cmsId = 0
      if (url.length>domain.length) {
        val cms = url.substring(url.indexOf(domain) + domain.length)
        val cmsTypeId = cms.split("/")
        cmsType = cmsTypeId(0)
        if (cmsTypeId(1).contains("?")){
            cmsId = cmsTypeId(1).split("[?]")(0).toInt
          }else{
            val p = """\d+""".r
          if((p findAllIn cmsTypeId(1) toList ) isEmpty){
            cmsId= 0
          }else{
            cmsId = cmsTypeId(1).toInt
          }
          }
      }
      val city =IpUtils.getCity(ip)
      val time = splits(0)
      val day = time.substring(0,10).replaceAll("-","")
      //Row 字段要和结构体字段对应
      Row(url,cmsType,cmsId,traffic,ip,city,time,day)
    }catch {
      case e: Exception=>
        Row("-","-",0,0,"-","-","-","-")
    }
  }
}
