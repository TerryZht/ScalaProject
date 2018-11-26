package com.imooc.log

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
  * Mysql 工具类
  */
object MysqlUtils {
  /**
    * 获取数据库链接
    */
  def getConnection()={
    DriverManager.getConnection("jdbc:mysql://192.168.199.128:3306/spark","root","4569630")
  }

  /**
    * 释放资源
    * @param connection
    * @param pstmt
    */
  def release(connection:Connection, pstmt:PreparedStatement): Unit ={
    try{
      if(pstmt != null){
        pstmt.close()
      }
    }catch {
      case e: Exception =>e.printStackTrace()
    }finally {
      connection.close()
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }
}
