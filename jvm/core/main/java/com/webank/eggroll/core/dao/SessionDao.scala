package com.webank.eggroll.core.dao
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta}
import com.webank.eggroll.core.meta.Meta.SessionMeta
import slick.jdbc.MySQLProfile.api._

import java.sql.Timestamp

//CREATE TABLE IF NOT EXISTS `session_main`
//(
//`session_id`        VARCHAR(767) PRIMARY KEY,
//`name`              VARCHAR(2000) NOT NULL DEFAULT '',
//`status`            VARCHAR(255)  NOT NULL,
//`tag`               VARCHAR(255),
//`total_proc_count`  INT,
//`active_proc_count` INT,
//`created_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
//`updated_at`        DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
//) DEFAULT CHARACTER SET latin1



//case class ErSessionMeta(id: String = StringConstants.EMPTY,
//                         name: String = StringConstants.EMPTY,
//                         status: String = StringConstants.EMPTY,
//                         totalProcCount: Int = 0,
//                         activeProcCount: Int = 0,
//                         tag: String = StringConstants.EMPTY,
//                         processors: Array[ErProcessor] = Array(),
//                         createTime:Timestamp = null,
//                         updateTime:Timestamp = null,
//                         options: Map[String, String] = Map())


 class SessionDao(tagInput: Tag) extends Table[ErSessionMeta](tagInput, "session_main"){
  def id = column[String]("session_id", O.PrimaryKey)
  def name = column[String]("name")
  def status = column[String]("status")
   def totalProcCount = column[Int]("total_proc_count")
   def  activeProcCount  = column[Int]("active_proc_count")
   def  tag =  column[String]("tag")
   def  createTime = column[Timestamp]("created_at")
   def  updateTime = column[Timestamp]("updated_at")

   def * = (id, name, status,totalProcCount,activeProcCount,tag, Array(),createTime,updateTime,Map()) <> (ErSessionMeta.tupled, ErSessionMeta.unapply)
 }
object SessionDao{



}