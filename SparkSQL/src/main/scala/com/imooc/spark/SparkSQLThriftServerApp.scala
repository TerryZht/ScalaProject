package com.imooc.spark

import java.sql.DriverManager

/**
  * 通过jdbc 访问spark
  */
object SparkSQLThriftServerApp {

  def main(args: Array[String]): Unit = {
         Class.forName("org.apache.hive.jdbc.HiveDriver")
        val conn =  DriverManager.getConnection("jdbc:hive2://Linux:10000","Linux","")
        val pstcnt = conn.prepareStatement("select * from a")
        val rs = pstcnt.executeQuery()
        while (rs.next()){
          println(rs.getInt("a"))
        }
      rs.close()
      pstcnt.close()
      conn.close()
  }
}
