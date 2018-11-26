package com.imooc.log

import com.ggstar.util.ip.IpHelper

object IpUtils {
  def getCity(ip:String)={
    val city = IpHelper.findRegionByIp(ip)
    city
  }

  def main(args: Array[String]): Unit = {
    println(getCity("119.130.229.90"))
  }
}
