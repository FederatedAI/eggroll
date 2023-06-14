package com.webank.eggroll.core.dao
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.{ErProcessor, ErSessionMeta, ErSessionMetaTest}
import com.webank.eggroll.core.meta.Meta.SessionMeta
import com.webank.eggroll.core.resourcemanager.RdbNew
import com.webank.eggroll.core.resourcemanager.RdbNew.dataSource
import slick.jdbc.MySQLProfile.api._

import java.sql.Timestamp
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

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
   def  createArray = ()=>{ Array()}
   def  createMap =()=>{Map()}

   def * = (id, name, status,totalProcCount,activeProcCount,tag,createTime,updateTime) <>[ErSessionMeta]
     (t=>ErSessionMeta(id=t._1,name=t._2,status=t._3,totalProcCount=t._4,activeProcCount=t._5,tag=t._6,createTime=t._7,updateTime = t._8),
       u=>Some((u.id,u.name,u.status,u.totalProcCount,u.activeProcCount,u.tag,u.createTime,u.updateTime))
     )
 }
object SessionDao{
  val sessionDao = TableQuery[SessionDao]
  val db = Database.forDataSource(dataSource, None)

  def insert(erSessionMeta: ErSessionMeta ) :Int = {
    val eventualInt = db.run(sessionDao += erSessionMeta)
    var count =0
    eventualInt.onComplete{
      case Success(count) => println("===============新增数据：" + count)
      case Failure(e) => println("===============An error has occured: " + e.getMessage)
    }
    return count
  }

  def selectOne(sessionId :String): ErSessionMeta = {
    val sessionGet = db.run(sessionDao.filter(_.id === sessionId).result)
    sessionGet.onComplete{
      case Success(data) =>{
      }
      case Failure(ex)   => throw ex
    }
    var session :ErSessionMeta =Await.result(sessionGet, 5 seconds).headOption.get
    return session
  }

  def update(session: ErSessionMeta ) :Int = {
    val eventualInt = db.run(sessionDao.filter(_.id === session.id).update(session))
    eventualInt.onComplete{
      case Success(count) => println("===============修改数据：" + count)
      case Failure(ex)   => throw ex
    }
    return 0
  }









  def selectByExample(sessionId :String): ErSessionMeta = {
    val sessionGet = db.run(sessionDao.filter(_. === sessionId).result)
    sessionGet.onComplete{
      case Success(data) =>{
      }
      case Failure(ex)   => throw ex
    }
    var session :ErSessionMeta =Await.result(sessionGet, 5 seconds).headOption.get
    return session
  }





}