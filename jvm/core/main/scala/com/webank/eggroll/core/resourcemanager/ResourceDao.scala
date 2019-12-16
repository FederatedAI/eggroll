package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.ClusterManagerConfKeys
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErSessionDeployment, ErSessionMeta}
import com.webank.eggroll.core.meta.Meta.SessionMeta
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.JdbcTemplate
import com.webank.eggroll.core.util.JdbcTemplate.ResultSetIterator
import org.apache.commons.dbcp2.BasicDataSource

import scala.collection.JavaConverters._


class SessionMetaDao {
  private val dbc = new JdbcTemplate(ResourceDao.dataSource.getConnection)
  def register(sessionMeta: ErSessionMeta, replace:Boolean = true):Unit = {
    val sid = sessionMeta.id
    dbc.withTransaction{ conn =>
      if (replace) {
        dbc.update(conn, "delete from session_main where session_id=?", sid)
        dbc.update(conn, "delete from session_option where session_id=?", sid)
        dbc.update(conn, "delete from session_processor where session_id=?", sid)
      }
      dbc.update(conn,
        "insert into session_main(session_id, name, status, tag) values(?, ?, ?, ?)",
        sid, sessionMeta.name, sessionMeta.status, sessionMeta.tag)
      val opts = sessionMeta.options.asScala
      if(opts.nonEmpty) {
        val valueSql = ("(?, ?, ?) ," * opts.size).stripSuffix(",")
        val params = opts.flatMap{case (k,v) => Seq(sid, k, v)}.toSeq
        dbc.update(conn, "insert into session_option(session_id, name, data) values " + valueSql, params:_*)
      }
      val procs = if(sessionMeta.deployment != null) sessionMeta.deployment.rolls ++ sessionMeta.deployment.eggs.values.flatMap(_.toSeq) else Array[ErProcessor]()
      if(procs.nonEmpty) {
        val valueSql = ("(?, ?, ?, ?, ?, ?, ?)," * procs.length).stripSuffix(",")
        val params = procs.flatMap(proc => Seq(
          sid, proc.serverNodeId, proc.processorType, proc.status, proc.tag,
          proc.commandEndpoint.toString,
          proc.transferEndpoint.toString)).toSeq
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
    val procs = dbc.query( rs => rs.map(_ => ErProcessor(
          serverNodeId = rs.getInt("server_node_id"), name = rs.getString("name"),
          processorType = rs.getString("processor_type"), status = rs.getString("status"),
          commandEndpoint =ErEndpoint(rs.getString("command_endpoint")),
          transferEndpoint = ErEndpoint(rs.getString("transfer_endpoint")))
        ),
      "select * from session_processor where session_id = ?", sessionId)
//    val deploy = ErSessionDeployment()
    // TODO:0: refactor deploy
    println(procs.toList)
    dbc.query( rs => {
      if (!rs.next()) {
        throw new IllegalArgumentException("session id not found:" + sessionId)
      }
      ErSessionMeta(
        id = sessionId, name = rs.getString("name"),
        status = rs.getString("status"), tag = rs.getString("tag"),
        options = opts.asJava)
    },"select * from session_main where session_id = ?", sessionId)
  }
  def addSession(sessionMeta: ErSessionMeta): Unit = {
    if(dbc.queryOne("select * from session_main where session_id = ?", sessionMeta.id).nonEmpty) {
      throw new IllegalArgumentException("session exists:" + sessionMeta.id)
    }
    register(sessionMeta)
  }
}
object ResourceDao {
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
