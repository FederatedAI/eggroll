package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.{ClusterManagerConfKeys, ProcessorTypes}
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErServerCluster, ErServerNode, ErSessionDeployment, ErSessionMeta}
import com.webank.eggroll.core.meta.Meta.SessionMeta
import com.webank.eggroll.core.resourcemanager.ResourceDao.NotExistError
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.JdbcTemplate
import com.webank.eggroll.core.util.JdbcTemplate.ResultSetIterator
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters._

class ServerMetaDao {
  private val dbc = new JdbcTemplate(ResourceDao.dataSource.getConnection)
  def getServerCluster(clusterId:Long = 0): ErServerCluster = {
    val nodes = dbc.query(rs =>
      rs.map(_ => ErServerNode(
        id = rs.getLong("server_id"), clusterId=clusterId, name = rs.getString("name"),
        endpoint = ErEndpoint(host=rs.getString("host"), port = rs.getInt("port")),
        nodeType = rs.getString("node_type"), status = rs.getString("status")))
    ,"select * from server_node where server_cluster_id=?", clusterId)
    ErServerCluster(id = clusterId, serverNodes = nodes.toArray)
  }
}

class StoreMetaDao {

}

class SessionMetaDao {
  private val dbc = new JdbcTemplate(ResourceDao.dataSource.getConnection)
  def register(sessionMeta: ErSessionMeta, replace:Boolean = true):Unit = {
    require(sessionMeta.activeProcCount == sessionMeta.processors.count(_.status == RmConst.PROC_READY),
      "conflict active proc count:" + sessionMeta)
    val sid = sessionMeta.id
    dbc.withTransaction{ conn =>
      if (replace) {
        dbc.update(conn, "delete from session_main where session_id=?", sid)
        dbc.update(conn, "delete from session_option where session_id=?", sid)
        dbc.update(conn, "delete from session_processor where session_id=?", sid)
      }
      dbc.update(conn,
        "insert into session_main(session_id, name, status, tag, active_proc_count) values(?, ?, ?, ?, 0)",
        sid, sessionMeta.name, sessionMeta.status, sessionMeta.tag)
      val opts = sessionMeta.options
      if(opts.nonEmpty) {
        val valueSql = ("(?, ?, ?) ," * opts.size).stripSuffix(",")
        val params = opts.flatMap{case (k,v) => Seq(sid, k, v)}.toSeq
        dbc.update(conn, "insert into session_option(session_id, name, data) values " + valueSql, params:_*)
      }
      val procs = sessionMeta.processors
      if(procs.nonEmpty) {
        val valueSql = ("(?, ?, ?, ?, ?, ?, ?)," * procs.length).stripSuffix(",")
        val params = procs.flatMap(proc => Seq(
          sid, proc.serverNodeId, proc.processorType, proc.status, proc.tag,
          if(proc.commandEndpoint != null) proc.commandEndpoint.toString else "",
          if(proc.transferEndpoint != null) proc.transferEndpoint.toString else ""))
        dbc.update(conn,
                   "insert into session_processor(session_id, server_node_id, processor_type, status, " +
                   "tag, command_endpoint, transfer_endpoint) values " + valueSql,
                   params:_*)
      }
    }
  }

  def getSession(sessionId: String): ErSessionMeta = {
    val opts = dbc.query(
          rs => rs.map(
            _ => (rs.getString("name"), rs.getString("data"))
          ).toMap,
      "select * from session_option where session_id = ?", sessionId)
    val procs = dbc.query( rs => rs.map(_ =>
      ErProcessor(id = rs.getLong("processor_id"),
          serverNodeId = rs.getInt("server_node_id"),
          processorType = rs.getString("processor_type"), status = rs.getString("status"),
          commandEndpoint = if(StringUtils.isBlank(rs.getString("command_endpoint"))) null
                            else ErEndpoint(rs.getString("command_endpoint")),
          transferEndpoint = if(StringUtils.isBlank(rs.getString("transfer_endpoint"))) null
                              else ErEndpoint(rs.getString("transfer_endpoint")))
        ),
      "select * from session_processor where session_id = ?", sessionId)
    getSessionMain(sessionId).copy(options = opts, processors = procs.toList)
  }
  def addSession(sessionMeta: ErSessionMeta): Unit = {
    if(dbc.queryOne("select * from session_main where session_id = ?", sessionMeta.id).nonEmpty) {
      throw new NotExistError("session exists:" + sessionMeta.id)
    }
    register(sessionMeta)
  }

  def updateProcessor(proc: ErProcessor):Unit = {
    val (session_id:String, old:String) = dbc.query( rs => {
      if (!rs.next()) {
        throw new NotExistError("processor not exits:" + proc)
      }
      (rs.getString("session_id"), rs.getString("status"))
    }, "select session_id, status from session_processor where processor_id=?", proc.id)
    dbc.withTransaction{conn =>
      if(old == RmConst.PROC_READY && proc.status != RmConst.PROC_READY)  {
        dbc.update(conn, "update session_main set active_proc_count = active_proc_count - 1 where session_id = ?", session_id)
      } else if(old != RmConst.PROC_READY && proc.status == RmConst.PROC_READY) {
        dbc.update(conn, "update session_main set active_proc_count = active_proc_count + 1 where session_id = ?", session_id)
      }
      var sql = "update session_processor set status = ? where processor_id=?"
      var params = List(proc.status, proc.id)
      if(proc.transferEndpoint != null) {
        sql += " and transfer_endpoint=?"
        params ++= proc.transferEndpoint.toString
      }
      if(proc.commandEndpoint != null) {
        sql += " and command_endpoint = ?"
        params ++= proc.commandEndpoint.toString
      }
      dbc.update(conn, sql, params:_*)
    }
  }

  def getSessionMain(sessionId: String): ErSessionMeta = {
    dbc.query( rs => {
      if (!rs.next()) {
        throw new NotExistError("session id not found:" + sessionId)
      }
      ErSessionMeta(
        id = sessionId, name = rs.getString("name"),
        activeProcCount = rs.getInt("active_proc_count"),
        status = rs.getString("status"), tag = rs.getString("tag"))
    },"select * from session_main where session_id = ?", sessionId)
  }

  def exitsSession(sessionId: String): Boolean = {
    dbc.queryOne("select 1 from session_main where session_id = ?", sessionId).isEmpty
  }

  def updateSessionMain(sessionMeta: ErSessionMeta):Unit = {
    dbc.update("update session_main set name = ? , status = ? , tag = ? , active_processors = ?",
      sessionMeta.name, sessionMeta.status, sessionMeta.tag, sessionMeta.activeProcCount)
  }
}
object ResourceDao {
  class NotExistError(msg: String) extends Exception(msg)
  val dataSource: BasicDataSource = new BasicDataSource
  dataSource.setDriverClassName(StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME, "com.mysql.cj.jdbc.Driver"))
  dataSource.setUrl(StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_URL))
  dataSource.setUsername(StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME))
  dataSource.setPassword(StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_PASSWORD))
  dataSource.setMaxIdle(StaticErConf.getInt(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_IDLE, 10))
  dataSource.setMaxTotal(StaticErConf.getInt(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_TOTAL, 100))
  dataSource.setTimeBetweenEvictionRunsMillis(StaticErConf.getLong(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_TIME_BETWEEN_EVICTION_RUNS_MS, 10000L))
  dataSource.setMinEvictableIdleTimeMillis(StaticErConf.getLong(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MIN_EVICTABLE_IDLE_TIME_MS, 120000L))
  dataSource.setDefaultAutoCommit(StaticErConf.getBoolean(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_DEFAULT_AUTO_COMMIT, false))
}
