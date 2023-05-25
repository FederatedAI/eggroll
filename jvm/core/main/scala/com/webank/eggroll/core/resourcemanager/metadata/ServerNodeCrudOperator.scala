/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.core.resourcemanager.metadata

import com.webank.eggroll.core.ErSession

import java.util
import java.util.Date
import com.webank.eggroll.core.constant.{ResourceStatus, ServerNodeStatus, ServerNodeTypes}
import com.webank.eggroll.core.error.CrudException
import com.webank.eggroll.core.meta.{ErEndpoint, ErProcessor, ErResource, ErServerCluster, ErServerNode, ErSessionMeta}
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator.{dbc, doGetServerNodesWithResource, doUpdateServerNodeById}
import com.webank.eggroll.core.resourcemanager.BaseDao
import com.webank.eggroll.core.resourcemanager.BaseDao.NotExistError
import com.webank.eggroll.core.util.JdbcTemplate.ResultSetIterator
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

import java.sql.{Connection, ResultSet}
import scala.+:
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait CrudOperator

class ServerNodeCrudOperator extends CrudOperator with Logging {
  def getServerCluster(input: ErServerCluster): ErServerCluster = {
    ServerNodeCrudOperator.doGetServerCluster(input)
  }

  def getServerNode(input: ErServerNode): ErServerNode = synchronized {
    val nodeResult = ServerNodeCrudOperator.doGetServerNodes(input)

    if (nodeResult.nonEmpty) {
      nodeResult(0)
    } else {
      null
    }
  }

  def  createServerNode(input:ErServerNode) :ErServerNode = {
    ServerNodeCrudOperator.doCreateServerNode(input)
  }

  def updateServerNodeById(input: ErServerNode, isHeartbeat: Boolean = false): ErServerNode = {
    doUpdateServerNodeById(input,isHeartbeat)
  }



  def getOrCreateServerNode(input: ErServerNode): ErServerNode = synchronized {
    ServerNodeCrudOperator.doGetOrCreateServerNode(input)
  }

  def createOrUpdateServerNode(input: ErServerNode): ErServerNode = synchronized {
    def samFunctor(input: ErServerNode): ErServerNode = {
      ServerNodeCrudOperator.doCreateOrUpdateServerNode(input = input, isHeartbeat = false)
    }

    samFunctor(input)
  }



  def getServerNodesWithResource(input : ErServerNode):Array[ErServerNode] = {
    doGetServerNodesWithResource(input)
  }

  def getServerNodes(input: ErServerNode): ErServerCluster = synchronized {
    val serverNodes = ServerNodeCrudOperator.doGetServerNodes(input)

    if (serverNodes.nonEmpty) {
      ErServerCluster(id = 0, serverNodes = serverNodes)
    } else {
      null
    }
  }


  def getServerClusterByHosts(input: util.List[String]): ErServerCluster = synchronized {
    ServerNodeCrudOperator.doGetServerClusterByHosts(input)
  }
  def getNodeResources(serverNodeId: Long,rType:String): Array[ErResource] = synchronized {
    ServerNodeCrudOperator.doQueryNodeResources(serverNodeId,rType)
  }
  def updateNodeResource(connection: Connection,serverNodeId:Long,resources: Array[ErResource]) = synchronized {
    ServerNodeCrudOperator.doUpdateNodeResource(connection,serverNodeId,resources)
  }

  def updateNodeProcessor(connection: Connection, processors:Array[ErProcessor]) = synchronized {
    ServerNodeCrudOperator.doUpdateNodeProcessor(connection, processors)
  }


  def queryProcessorResourceList(connection: Connection, serverNodeId:Long,status:Array[String]):Array[ErResource]={
    ServerNodeCrudOperator.doQueryProcessorResourceList(connection,serverNodeId,status)
  }

  def updateSessionProcessCount(connection: Connection,session:ErSessionMeta): Unit= synchronized{

    ServerNodeCrudOperator.doUpdateSession(conn =  connection,session)
  }

  def insertNodeResource(connection: Connection,serverNodeId:Long,resources: Array[ErResource]) = synchronized{
    ServerNodeCrudOperator.doInsertNodeResource(connection,serverNodeId,resources)
  }
  def insertProcessorResource(connection: Connection,processors:Array[ErProcessor]) = synchronized{
    ServerNodeCrudOperator.doInsertProcessorResource(connection,processors)
  }
  def queryProcessorResource(connection: Connection,processor:ErProcessor,status:String ) = synchronized{
    ServerNodeCrudOperator.doQueryProcessorResources(connection,processor.id,status)
  }
  def updateProcessorResource(connection: Connection,processor:ErProcessor):Unit = {
    ServerNodeCrudOperator.doUpdateProcessorResource(connection,processor);
  }

  def queryProcessor(connection: Connection,processor:ErProcessor):Array[ErProcessor]={
    ServerNodeCrudOperator.doQueryProcessor(connection,processor)
  }

  def allocateNodeResource(connection: Connection,serverNodeId:Long,resources: Array[ErResource]):Unit = synchronized{
    ServerNodeCrudOperator.doAllocateNodeResource(connection,serverNodeId,resources)
  }

  def preAllocateNodeResource(connection: Connection,serverNodeId:Long,resources: Array[ErResource]):Unit = synchronized{
    ServerNodeCrudOperator.doPreAllocateNodeResource(connection,serverNodeId,resources)
  }


  def returnNodeResource(connection: Connection,serverNodeId:Long,resources: Array[ErResource]):Unit = synchronized{
    ServerNodeCrudOperator.doReturnNodeResource(connection,serverNodeId,resources)
  }


  def countNodeResource(connection:Connection,serverNodeId:Long ):Array[ErResource]= synchronized{

    ServerNodeCrudOperator.doCountNodeResource(connection,serverNodeId)

  }



  def queryNodeResourceDetail(connection:Connection,serverNodeId:Long ,statusList:Array[String]):Array[ErResource]={

    ServerNodeCrudOperator.doCountNodeResource(connection,serverNodeId)

  }



}

object ServerNodeCrudOperator extends Logging {
   lazy val dbc = BaseDao.dbc
  private[metadata] def doGetServerCluster(input: ErServerCluster): ErServerCluster = {
    val sql = "select * from server_node where server_cluster_id = ?"
    val nodeResult = dbc.query(rs =>
      rs.map(_ => ErServerNode(
        id = rs.getInt("server_node_id"), 
        name = rs.getString("name"),
        endpoint = ErEndpoint(host=rs.getString("host"), port = rs.getInt("port")))
      ), sql, input.id)

    if (nodeResult.isEmpty) return null

    ErServerCluster(serverNodes = nodeResult.toArray)
  }

def doCreateServerNode(input: ErServerNode): ErServerNode = {
    val nodeRecord = dbc.withTransaction(conn => {
      var id_name:String = if(input.id>0) "server_node_id , " else ""
      var id_param:String = if(input.id>0) " ? ," else ""
      val sql = "insert into server_node ("+id_name+" name, server_cluster_id, host, port, node_type, status)"+
        " values ("+  id_param+ "?, ?, ?, ?, ?, ?)"
      var params =List[Any]()
      var tempParams = mutable.ListBuffer(input.name, input.clusterId,
        input.endpoint.host, input.endpoint.port,input.nodeType, input.status)
      if(input.id>0) {
           params ++= Array(input.id)
      }
      params++= tempParams

      println(input.id+"========="+params)
      val id = dbc.update(conn, sql,params.toList:_*)
//        input.name, input.clusterId,
//        input.endpoint.host, input.endpoint.port, input.nodeType,input.status)

      id
    })

    if (nodeRecord.isEmpty) {
      throw new CrudException(s"Illegal rows affected when creating node: ${nodeRecord}")
    }

    ErServerNode(
      id = nodeRecord.get,
      name = input.name,
      clusterId = input.clusterId,
      endpoint = ErEndpoint(host = input.endpoint.host, port = input.endpoint.port),
      nodeType = input.nodeType,
      status = input.status,
      resources = input.resources)
  }

  private[metadata] def existSession(sessionId: Long): Boolean = {
    dbc.queryOne("select * from session_main where session_id = ?", sessionId).nonEmpty
  }

  private def doUpdateServerNodeById(input: ErServerNode, isHeartbeat: Boolean = false): ErServerNode = {
    val nodeRecord = dbc.withTransaction(conn => {
      var sql = "update server_node set " +
        " host = ?, port = ?,  status = ? "
      var params = List(
        input.endpoint.host, input.endpoint.port, input.status)

      if (isHeartbeat) {
        sql += ", last_heartbeat_at = ? "
        params ++= Array(new Date(System.currentTimeMillis()))
      }
      sql+="where server_node_id = ?"
      params ++= Array(input.id)
      //logInfo(s"doUpdateServerNodeById  ${sql} : ${params} ")
      dbc.update(conn, sql, params:_*)
    })

//    if (nodeRecord.isEmpty) {
//      throw new CrudException(s"Illegal rows affected when creating node: ${nodeRecord}")
//    }
    ErServerNode(
      id = input.id,
      name = input.name,
      clusterId = input.clusterId,
      endpoint = ErEndpoint(host = input.endpoint.host, port = input.endpoint.port),
      nodeType = input.nodeType,
      status = input.status)

  }

  private[metadata] def doGetServerNodesUnwrapped(input: ErServerNode): Array[DbServerNode] = {
    var sql = "select * from server_node where 1=? "
    var params = ListBuffer("1")

    if (input.id > 0) {
      sql += "and server_node_id = ?"
      params ++= Array(input.id.toString)
    }

    if (!StringUtils.isBlank(input.name)) {
      sql += "and name = ?"
      params ++= Array(input.name)
    }

    if (input.clusterId >= 0) {
      sql += "and server_cluster_id = ?"
      params ++= Array(input.clusterId.toString)
    }

    if (!StringUtils.isBlank(input.endpoint.host)) {
      sql += "and host = ?"
      params ++= Array(input.endpoint.host)
    }

    if (input.endpoint.port > 0) {
      sql += "and port = ?"
      params ++= Array(input.endpoint.port.toString)
    }

    if (!StringUtils.isBlank(input.nodeType)) {
      sql += "and node_type = ?"
      params ++= Array(input.nodeType)
    }

    if (!StringUtils.isBlank(input.status)) {
      sql += "and status = ?"
      params ++= Array(input.status)
    }

    val nodeResult = dbc.query( rs => rs.map(_ => DbServerNode(
      id = rs.getLong("server_node_id"),
      name = rs.getString("name"),
      clusterId = rs.getLong("server_cluster_id"),
      endpoint = ErEndpoint(host=rs.getString("host"), port = rs.getInt("port")),
      nodeType = rs.getString("node_type"),
      status = rs.getString("status"),
      lastHeartbeatAt = rs.getDate("last_heartbeat_at"),
      createdAt = rs.getDate("created_at"),
      updatedAt = rs.getDate("updated_at"))), sql, params:_*).toList

    nodeResult.toArray
  }

  def doGetServerNodesWithResource(input: ErServerNode): Array[ErServerNode] = {
    var  serverNodes =  doGetServerNodes(input)
    serverNodes.map(node=>{
      var resouces =  doQueryNodeResources(node.id)

      node.copy(resources = resouces)

    })
  }



  def doGetServerNodes(input: ErServerNode): Array[ErServerNode] = {

    var sql = "select * from server_node where 1=? "
    var params = List("1")

    if (input.id > 0) {
      sql += "and server_node_id = ?"
      params ++= Array(input.id.toString)
    }

    if (!StringUtils.isBlank(input.name)) {
      sql += "and name = ?"
      params ++= Array(input.name)
    }

    if (input.clusterId >= 0) {
      sql += "and server_cluster_id = ?"
      params ++= Array(input.clusterId.toString)
    }

    if (!StringUtils.isBlank(input.endpoint.host)) {
      sql += "and host = ?"
      params ++= Array(input.endpoint.host)
    }

    if (input.endpoint.port > 0) {
      sql += "and port = ?"
      params ++= Array(input.endpoint.port.toString)
    }

    if (!StringUtils.isBlank(input.nodeType)) {
      sql += "and node_type = ?"
      params ++= Array(input.nodeType)
    }

    if (!StringUtils.isBlank(input.status)) {
      sql += "and status = ?"
      params ++= Array(input.status)
    }

    sql += "order by server_node_id asc"
   // logInfo(s"doGetServerNodes sql : ${sql}  param ${params}");
    val nodeResult = dbc.query(rs => rs.map(_ =>
      ErServerNode(
        id = rs.getLong("server_node_id"),
        name = rs.getString("name"),
        clusterId = rs.getLong("server_cluster_id"),
        endpoint = ErEndpoint(host = rs.getString("host"), port = rs.getInt("port")),
        nodeType = rs.getString("node_type"),
        status = rs.getString("status"),
        lastHeartBeat = rs.getTimestamp("last_heartbeat_at")
      )), sql, params: _*)

    nodeResult.toArray

  }

  private[metadata] def doGetOrCreateServerNode(input: ErServerNode): ErServerNode = {
    val existing = ServerNodeCrudOperator.doGetServerNodes(input)

    if (existing.nonEmpty) {
      existing(0)
    } else {
      doCreateServerNode(input)
    }
  }

  def doQueryProcessorResources(connection: Connection,processId:Long,status :String ):Array[ErResource]= {
    var sql = "select * from processor_resource where 1=1 "
    var params = List[String]()
    if(StringUtils.isNotEmpty(status)){
      params = params:+ status
      sql+= " and status =? "
    }

    if (processId!= -1) {
      params = params:+processId.toString
      sql+= s" and processor_id=? "
    }


//    logDebug(s"sql: ${sql} param: ${params}")
    val resourceResult =  dbc.query(connection,rs => rs.map(_ =>
      ErResource(
       // resourceId = rs.getLong("resource_id"),
        resourceType = rs.getString("resource_type"),
//        total = rs.getLong("total"),
//        used = rs.getLong("used"),
        allocated =  rs.getLong("allocated"),
        status = rs.getString("status"))),
      sql, params: _*)
    resourceResult.toArray

  }

  def doQueryProcessor(connection: Connection,erProcessor: ErProcessor) :Array[ErProcessor]={
    var sql = "select * from session_processor where 1=1 "

    var params = List[String]()
    if (StringUtils.isNotEmpty(erProcessor.status)) {
      params = params :+ erProcessor.status
      sql += " and status =? "
    }
    if (StringUtils.isNotEmpty(erProcessor.sessionId)) {
      params = params :+ erProcessor.sessionId
      sql += " and session_id =? "
    }

    if (erProcessor.id != -1) {
      params = params :+ erProcessor.id.toString
      sql += s" and processor_id=? "
    }
     // logInfo(s" =========${sql}=========${params}")
    var func: ResultSet => Iterable[ErProcessor]=rs
    => rs.map(_ =>
      ErProcessor(id = rs.getLong("processor_id"),
        serverNodeId = rs.getInt("server_node_id"),
        sessionId = rs.getString("session_id"),
        processorType = rs.getString("processor_type"), status = rs.getString("status"),
        commandEndpoint = if (StringUtils.isBlank(rs.getString("command_endpoint"))) null
        else ErEndpoint(rs.getString("command_endpoint")),
        transferEndpoint = if (StringUtils.isBlank(rs.getString("transfer_endpoint"))) null
        else ErEndpoint(rs.getString("transfer_endpoint")),
        pid = rs.getInt("pid"),
        createdAt = rs.getTimestamp("created_at"),
        updatedAt = rs.getTimestamp("updated_at")))
   val resourceResult :Iterable[ErProcessor]= null
    if(connection!=null){
     dbc.query(connection, func, sql, params: _*).toArray
    }else{
     dbc.query(func, sql, params: _*).toArray
    }
  }


  def doQueryNodeResources(serverNodeId:Long,rType:String=""): Array[ErResource]= {

    var sql = "select * from node_resource where 1=? "
    var params = List("1")
    if (!StringUtils.isBlank(rType)) {
      params = params:+rType
      sql+= s" and type=? "
    }
    if(serverNodeId>=0){
      params = (params:+serverNodeId.toString)
      sql+= s" and server_node_id = ?"
    }
//    val nodeResult = dbc.query(rs => rs.map(_ =>
//      ErServerNode(
//        id = rs.getLong("server_node_id"),
//        name = rs.getString("name"),
//        endpoint = ErEndpoint(host = rs.getString("host"), port = rs.getInt("port")))),
//      sql.toString(), inputListBuffer: _*)


    val resourceResult =  dbc.query(rs => rs.map(_ =>{
      ErResource(
        resourceId = rs.getLong("resource_id"),
        resourceType = rs.getString("resource_type"),
        total = rs.getLong("total"),
        used = rs.getLong("used"),
        allocated =  rs.getLong("allocated"),
        extention = rs.getString("extention"),
        status = rs.getString("status"))}),
      sql, params: _*)
    resourceResult.toArray
  }

  def doCreateOrUpdateServerNode(input: ErServerNode, isHeartbeat: Boolean = false):  ErServerNode = synchronized{
    val existing = ServerNodeCrudOperator.doGetServerNodes(ErServerNode(id = input.id,clusterId = input.clusterId,endpoint = input.endpoint))
    if (existing.nonEmpty) {
      var  existNode = existing(0)
      var param = input.copy(id= existNode.id)
      doUpdateServerNodeById(param, isHeartbeat)
    } else {
      doCreateServerNode(input)
    }
  }

  def doGetServerClusterByHosts(input: util.List[String]): ErServerCluster = {
    val inputListBuffer = input.asScala

    var first = true
    val sql = new StringBuilder()
    sql.append(s"select * from server_node where status = '${ServerNodeStatus.HEALTHY}'")
      .append(s" and node_type = '${ServerNodeTypes.NODE_MANAGER}'")
      .append(s" and host in (")
    inputListBuffer.foreach(_ => {
      if (first) first = false else sql.append(", ")
      sql.append("?")
    })
    sql.append(") order by server_node_id asc")

    val nodeResult = dbc.query(rs => rs.map(_ =>
      ErServerNode(
        id = rs.getLong("server_node_id"),
        name = rs.getString("name"),
        endpoint = ErEndpoint(host = rs.getString("host"), port = rs.getInt("port")))),
      sql.toString(), inputListBuffer:_*)

    ErServerCluster(serverNodes = nodeResult.toArray)
  }

  def  registerResource(serverNodeId:Long ,insertData:Array[ErResource],updateData:Array[ErResource],deleteData:Array[ErResource]): Unit = {
    dbc.withTransaction(conn => {
        if(insertData!=null&&insertData.length>0){
            doInsertNodeResource(conn,serverNodeId,insertData)
        }
        if(updateData!=null&&updateData.length>0){
            doUpdateNodeResource(conn,serverNodeId,updateData)
        }
        if(deleteData!=null&&deleteData.length>0){
            doDeleteNodeResource(conn,serverNodeId,deleteData)
        }
    })

  }

  def doInsertNodeResource(conn: Connection,serverNodeId:Long,resources: Array[ErResource])= synchronized{
    require(resources.length > 0)
    logInfo(s"insertNodeResource======== ${serverNodeId} ,size ${resources}")
   // dbc.withTransaction(conn => {
      resources.foreach(erResource => {

        try {
          dbc.update(conn,"insert node_resource ( server_node_id,resource_type,total,used,status) values (?,?,?,?,?)", serverNodeId, erResource.resourceType,
            erResource.total, erResource.used, erResource.status)
        } catch {
          case e: Exception =>

            println("got it:" + e.getMessage)
        }

//      })
    })
  }


  def doUpdateNodeProcessor(conn:Connection,processors :Array[ErProcessor]): Unit = synchronized{
    processors.foreach(erProcessor=>{
      var sql = "update session_processor  set status = ? "
      var first = false;
      var params = List[String]()
      params = (params :+ erProcessor.status)

      if (erProcessor.pid != -1) {
        params = (params :+ erProcessor.pid.toString)
        sql += " , pid = ?"

      }
      sql += " where  processor_id= ?"
      params = params:+erProcessor.id.toString
      dbc.update(conn, sql,
        params: _*)
    })
  }

  def  doUpdateSession(conn:Connection,sessionMeta:ErSessionMeta) :Unit = synchronized{

    var sql = "update session_main  set  "
    var first = false;
    var params = List[String]()
    if (sessionMeta.activeProcCount != -1) {
      params = (params :+ sessionMeta.activeProcCount.toString)
      sql += "  active_proc_count = ?"
      first = true;
    }
    sql += " where  session_id= ?"
    params = params :+sessionMeta.id.toString
    dbc.update(conn, sql,
      params: _*)



  }



  def doUpdateNodeResource(conn:Connection,serverNodeId:Long,resources: Array[ErResource]) = synchronized {

   // dbc.withTransaction(conn => {
      resources.foreach(erResource => {
        //update node_resource node_resource set    allocated = -1 where  1= 1 and resource_type=6  and server_node_id = -1", params=('-1','6','-1','1','VCPU_CORE')
      //  update node_resource  set    allocated = -1 where  1= 1 and resource_type=8  and server_node_id = -1", params=('-1','8','-1','1','VCPU_CORE')

        var sql = "update node_resource  set  "
        var  first = false;
        var params = List[String]()
        var sqlParams = List[String]()
        if (erResource.total >= 0) {
          params = (params :+ erResource.total.toString)
          sqlParams=sqlParams:+ "  total = ?"

          first = true;
        }
        if (erResource.allocated>= 0) {
          params = (params :+ erResource.allocated.toString)

          sqlParams=sqlParams :+ "  allocated = ?"

        }

        if(erResource.preAllocated>=0){
          params = (params :+ erResource.preAllocated.toString)

          sqlParams=sqlParams :+ "  pre_allocated = ?"
        }


        if (erResource.used >= 0) {
          params = (params :+ erResource.used.toString)

          sqlParams=sqlParams :+ "  used = ?"
        }
        if(erResource.extention!=null) {
          params = (params :+ erResource.extention)
          sqlParams = sqlParams :+ "  extention = ?"
        }


        sql+=    sqlParams.mkString(" , ")+ " where  1= 1"


        if (!StringUtils.isBlank(erResource.resourceType)) {
          params = params :+ erResource.resourceType
          sql += " and resource_type=? "
        }
        if (serverNodeId >= 0) {
          params = (params :+ serverNodeId.toString)
          sql += s" and server_node_id = ?"
        }


         // logInfo(s"========sql=======${sql}==== param ${params}")

          dbc.update(conn ,sql,
            params:_*)
      })
   // })
  }
  def  doAllocateNodeResource(conn:Connection,serverNodeId:Long,resources: Array[ErResource]): Unit = synchronized {
    //dbc.withTransaction(conn => {
      resources.foreach(erResource => {
        dbc.update(conn ,"update node_resource set allocated = allocated + ? , pre_allocated = pre_allocated - ?   where server_node_id = ? and resource_type = ? ",
          erResource.allocated,erResource.allocated, serverNodeId, erResource.resourceType)
      })
    //})
  }


  def  doPreAllocateNodeResource(conn:Connection,serverNodeId:Long,resources: Array[ErResource]): Unit = synchronized {
    //dbc.withTransaction(conn => {
    resources.foreach(erResource => {
      dbc.update(conn ,"update node_resource set pre_allocated = pre_allocated + ? where server_node_id = ? and resource_type = ? ",
        erResource.allocated, serverNodeId, erResource.resourceType)
    })
    //})
  }




  def  doReturnNodeResource(conn:Connection,serverNodeId:Long,resources: Array[ErResource]): Unit = synchronized {
   // dbc.withTransaction(conn => {
      resources.foreach(erResource => {
        dbc.update(conn ,"update node_resource set allocated = allocated - ? where server_node_id = ? and resource_type = ? ",
          erResource.allocated, serverNodeId, erResource.resourceType)
      })
   // })
  }



  def doQueryProcessorResourceList(conn:Connection,serverNodeId:Long ,status :Array[String]):Array[ErResource]={
    var params = List(serverNodeId)
    var statusString =status.mkString("('","','","')")

    var  sql = "select server_node_id, resource_type,extention, status, allocated from processor_resource where server_node_id = ? and status in "+statusString

    val resourceResult = dbc.query(conn,rs => rs.map(_=>
      ErResource(
        // resourceId = rs.getLong("resource_id"),
        resourceType = rs.getString("resource_type"),
        //total = rs.getLong("total"),
        //used = rs.getLong("used"),
        extention = rs.getString("extention"),
        serverNodeId = rs.getLong("server_node_id"),
        allocated = rs.getLong("allocated"),
        status = rs.getString("status"))),
      sql,params:_*).toArray

    resourceResult


  }

  def doCountNodeResource(conn:Connection,serverNodeId: Long  ):Array[ErResource]= synchronized {
    var params = List(serverNodeId)
    var  sql = "select server_node_id, resource_type, status, sum(allocated) as allocated from processor_resource where server_node_id = ? group by resource_type,status"


//    val resourceResult = dbc.query(conn, rs => rs.map(_ =>
//      ErResource(
//        // resourceId = rs.getLong("resource_id"),
//        resourceType = rs.getString("resource_type"),
//        //        total = rs.getLong("total"),
//        //        used = rs.getLong("used"),
//        allocated = rs.getLong("allocated"),
//        status = rs.getString("status"))),
//      sql, params: _*)


    val resourceResult = dbc.query(conn,rs => rs.map(_=>
      ErResource(
     // resourceId = rs.getLong("resource_id"),
      resourceType = rs.getString("resource_type"),
      //total = rs.getLong("total"),
      //used = rs.getLong("used"),
      serverNodeId = rs.getLong("server_node_id"),
      allocated = rs.getLong("allocated"),
      status = rs.getString("status"))),
      sql,params:_*).toArray
    resourceResult
  }

  def  doUpdateProcessorResource(connection: Connection ,processor: ErProcessor):Unit = {
        processor.resources.foreach(r=>{
          var sql = "update processor_resource set " +
            " status = ?, allocated = ?  where processor_id = ? and resource_type = ? "
          var params = List(r.status,r.allocated,processor.id,r.resourceType)
          dbc.update(connection, sql, params:_*)
        })
  }


  def  doInsertProcessorResource(connection: Connection,processors:Array[ErProcessor]) :Unit = synchronized {
      processors.foreach(erProcessor => {
        erProcessor.resources.foreach(resource => {
          dbc.update(connection, "insert into  processor_resource (processor_id,session_id,server_node_id,resource_type,allocated,status,extention)  values (?, ?,?, ?, ?,?,?)",
            erProcessor.id, erProcessor.sessionId, erProcessor.serverNodeId, resource.resourceType, resource.allocated,resource.status,resource.extention)
        })
    })
  }
  def doDeleteNodeResource(connection: Connection,serverNodeId: Long, resources: Array[ErResource]) = synchronized {
      resources.foreach(erResource => {
        dbc.update(connection ,"delete from node_resource  where server_node_id = ? and resource_type = ? ",
          serverNodeId, erResource.resourceType)
      })
  }
}
