package com.imooc.log



import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

object StatDAO {
  def insertVideoAccessTopN(list:ListBuffer[DayVideoAcessStat])={
    var connection:Connection = null
    var pstmt: PreparedStatement = null
      try{
        connection = MysqlUtils.getConnection()
        connection.setAutoCommit(false) //设置自动提交为关闭
        val sql = "insert into day_video_access_topn_stat(day,cms_id,cnt) values(?,?,?)"
        pstmt = connection.prepareStatement(sql)
        for(ele<-list){
          pstmt.setString(1,ele.day)
          pstmt.setString(2,ele.cmsId)
          pstmt.setLong(3,ele.times)
          pstmt.addBatch()
        }
        pstmt.executeBatch()//执行批量处理
        connection.commit() //手动提交
      }catch{
         case e:Exception=>e.printStackTrace()
      }finally {
          MysqlUtils.release(connection,pstmt)
      }
  }

  def insertCityAccessTopN(list:ListBuffer[DayCityAccessStat])={
    var connection:Connection = null
    var pstmt: PreparedStatement = null
    try{
      connection = MysqlUtils.getConnection()
      connection.setAutoCommit(false) //设置自动提交为关闭
      val sql = "insert into day_video_city_access_topn_stat(day,cms_id,city,times,rank) values(?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for(ele2<-list){
        pstmt.setString(1,ele2.day)
        pstmt.setLong(2,ele2.cmsId)
        pstmt.setString(3,ele2.city)
        pstmt.setLong(4,ele2.times)
        pstmt.setLong(5,ele2.rank)

        pstmt.addBatch()
      }
      pstmt.executeBatch()//执行批量处理
      connection.commit() //手动提交
    }catch{
      case e:Exception=>e.printStackTrace()
        println("dao")
    }finally {
      MysqlUtils.release(connection,pstmt)
    }
    def deleteData(day:String):Unit={
      val tables = Array("day_video_access_topn_stat","day_video_city_access_topn_stat")
      var connection:Connection = null
      var pstmt :PreparedStatement= null
      try{
        connection = MysqlUtils.getConnection()
        for (table<-tables){
          val sql = s"delete from $table where day = ?"
          pstmt = connection.prepareStatement(sql)
          pstmt.setString(1,day)
          pstmt.executeUpdate()
        }
      }catch {
        case e =>e.printStackTrace()
      }
      finally {
        MysqlUtils.release(connection,pstmt)
      }
    }
  }
}
