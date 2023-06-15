package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_NODE_HEARTBEAT_EXPIRED_COUNT
import com.webank.eggroll.core.constant.NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL
import com.webank.eggroll.core.constant.SessionConfKeys.{EGGROLL_SESSION_MAX_LIVE_TIME, EGGROLL_SESSION_STOP_TIMEOUT_MS}
import com.webank.eggroll.core.constant.{ProcessorStatus, ServerNodeStatus, SessionConfKeys, SessionStatus}
import com.webank.eggroll.core.deepspeed.job.JobServiceHandler
import com.webank.eggroll.core.deepspeed.job.JobServiceHandler.killJob
import com.webank.eggroll.core.error.ErSessionException
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.resourcemanager.ProcessorStateMachine.defaultSessionCallback
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.util.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait ClusterManager {
  //  def registerResource(data: ErServerNode): ErServerNode
  def nodeHeartbeat(data: ErNodeHeartbeat): ErNodeHeartbeat
}

object ClusterManagerService extends Logging {

  var nodeHeartbeatMap = mutable.Map[Long, ErNodeHeartbeat]()
  lazy val serverNodeCrudOperator = new ServerNodeCrudOperator()
  private lazy val smDao = new SessionMetaDao()


  private def checkNodeProcess(nodeManagerEndpoint: ErEndpoint, processor: ErProcessor): ErProcessor = {
    var result: ErProcessor = null
    try {
      var nodeManagerClient = new NodeManagerClient(nodeManagerEndpoint)
      result = nodeManagerClient.checkNodeProcess(processor)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    result

  }

//  private val stoppedNodeProcessChecker = new Thread(() => {
//    var serverNodeCrudOperator = new ServerNodeCrudOperator
//    while (true) {
//      try {
//        var now = System.currentTimeMillis();
//        var erProcessors = serverNodeCrudOperator.queryProcessor(null, null,extention = ()=>{
//         var  result =   List[(String,String)]()
//
//
//
//          result.put("and status in ( ?",ProcessorStatus)
//
//          var sql = " and status in ( ?, ?) "
//          params = params :+ ProcessorStatus.STOPPED
//
//          params.add
//
//
//
//        });
//        var grouped = erProcessors.groupBy(x => {
//          x.serverNodeId
//        })
//        grouped.par.
//          foreach(e => {
//            var serverNode = serverNodeCrudOperator.getServerNode(ErServerNode(id = e._1))
//            var nodeManagerClient = new NodeManagerClient(serverNode.endpoint)
//            e._2.par.foreach(processor => {
//              try {
//
//                var result = nodeManagerClient.checkNodeProcess(processor)
//                if (result == null || result.status == ProcessorStatus.KILLED) {
//                  Thread.sleep(10000)
//                  var processorInDb = serverNodeCrudOperator.queryProcessor(null, ErProcessor(id = processor.id))
//                  if (processorInDb.length > 0) {
//                    if (processorInDb.apply(0).status == ProcessorStatus.RUNNING) {
//                      var result = nodeManagerClient.checkNodeProcess(processor)
//                      if (result == null || result.status == ProcessorStatus.KILLED) {
//                        ProcessorStateMachine.changeStatus(processor, desStateParam = ProcessorStatus.ERROR)
//                      }
//                    }
//                  }
//                }
//              } catch {
//                case e: Throwable =>
//                  e.printStackTrace()
//              }
//            })
//          })
//      }
//      catch {
//        case e: Throwable =>
//          e.printStackTrace()
//      }
//      Thread.sleep(CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL.get().toInt)
//    }
//  }
//    ,"NODE_PROCESS_CHECK_THREAD")




  private val runningNodeProcessChecker = new Thread(() => {
    var serverNodeCrudOperator = new ServerNodeCrudOperator
    while (true) {
      try {
        var now = System.currentTimeMillis();
        var erProcessors = serverNodeCrudOperator.queryProcessor(null, ErProcessor(status = ProcessorStatus.RUNNING));
        var grouped = erProcessors.groupBy(x => {
          x.serverNodeId
        })
        grouped.par.
          foreach(e => {
            var serverNode = serverNodeCrudOperator.getServerNode(ErServerNode(id = e._1))
            var nodeManagerClient = new NodeManagerClient(serverNode.endpoint)
            e._2.par.foreach(processor => {
              try {

                var result = nodeManagerClient.checkNodeProcess(processor)
                if (result == null || result.status == ProcessorStatus.KILLED) {
                  Thread.sleep(10000)
                  var processorInDb = serverNodeCrudOperator.queryProcessor(null, ErProcessor(id = processor.id))
                  if (processorInDb.length > 0) {
                    if (processorInDb.apply(0).status == ProcessorStatus.RUNNING) {
                      var result = nodeManagerClient.checkNodeProcess(processor)
                      if (result == null || result.status == ProcessorStatus.KILLED) {
                        ProcessorStateMachine.changeStatus(processor, desStateParam = ProcessorStatus.ERROR)
                      }
                    }
                  }
                }
              } catch {
                case e: Throwable =>
                  e.printStackTrace()
              }
            })
          })
      }
      catch {
        case e: Throwable =>
          e.printStackTrace()
      }
      Thread.sleep(CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL.get().toInt)
    }
  }
  ,"NODE_PROCESS_CHECK_THREAD")


  private def checkAndHandleDeepspeedOutTimeSession(session: ErSessionMeta, sessionProcessors: Array[ErProcessor]): Unit = {
    var current = System.currentTimeMillis()
    var maxInterval = 2 * SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get().toLong
    var interval = current - session.createTime.getTime
    logDebug(s"watch deepspeed new session: ${session.id} ${interval}  ${maxInterval}")
    if (interval > maxInterval) {
      JobServiceHandler.killJob(session.id, isTimeout = true)
    }
  }

  private def checkAndHandleEggpairOutTimeSession(session: ErSessionMeta, sessionProcessors: Array[ErProcessor]): Unit = {
    var current = System.currentTimeMillis()
    if (session.createTime.getTime < current - 2 * SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get().toLong) {
      //sessionMeta: ErSessionMeta, afterState: String
      SessionManagerService.killSession(session, SessionStatus.KILLED)
    }
  }


  private def checkAndHandleEggpairActiveSession(session: ErSessionMeta, sessionProcessors: Array[ErProcessor]): Unit = {
    var  now = System.currentTimeMillis()
    if(now - session.createTime.getTime>EGGROLL_SESSION_MAX_LIVE_TIME.get().toInt){
      logInfo(s"session timeout ${session.id} ,live time ${now - session.createTime.getTime}, prepare to kill" )
      SessionManagerService.killSession(session)
    }else{
      var invalidProcessor = sessionProcessors.filter(p => {
        p.status == ProcessorStatus.ERROR ||
          p.status == ProcessorStatus.KILLED ||
          p.status == ProcessorStatus.STOPPED
      })
      if (invalidProcessor.length > 0) {
        var now = System.currentTimeMillis()
        var needKillSession: Boolean = false
        needKillSession = invalidProcessor.exists(p => p.updatedAt.getTime < now - EGGROLL_SESSION_STOP_TIMEOUT_MS.get().toLong)
        if (needKillSession) {
          logInfo(s"invalid processors ${invalidProcessor},session watcher kill eggpair session ${session} ");
          SessionManagerService.killSession(session)
        }
      }
    }
  }


  private def checkAndHandleDeepspeedActiveSession(session: ErSessionMeta, sessionProcessors: Array[ErProcessor]): Unit = {
    logInfo(s"checkAndHandleDeepspeedActiveSession ${session.id} ${sessionProcessors.mkString(",")}")


    if (sessionProcessors.exists(_.status == ProcessorStatus.ERROR)) {
      logInfo(s"session watcher kill session ${session}")
      try {
        killJob(session.id, isTimeout = false)
      } catch {
        case e: ErSessionException =>
          logError(s"failed to kill session ${session.id}", e)
      }

    } else if (sessionProcessors.forall(_.status == ProcessorStatus.FINISHED)) {
      smDao.updateSessionMain(session.copy(status = SessionStatus.FINISHED), afterCall = defaultSessionCallback)
      logDebug(s"found all processor belongs to session ${session.id} finished, " +
        s"update session status to `Finished`")
    }
  }




  private val sessionWatcher = {
    /*
    update session status according to processor status
     */
    new Thread(
      () => {
        while (true) {
          try {
            val sessions = smDao.getSessionMainsByStatus(Array(SessionStatus.ACTIVE, SessionStatus.NEW))
            sessions.foreach { session =>
              try {
                val sessionProcessors = smDao.getSession(session.id).processors
                session.name match {
                  case "DeepSpeed" =>
                    logDebug(s"watch deepspeed session: ${session.id} ${session.status}")
                    session.status match {
                      case SessionStatus.ACTIVE =>
                        checkAndHandleDeepspeedActiveSession(session, sessionProcessors)
                      case SessionStatus.NEW =>
                        checkAndHandleDeepspeedOutTimeSession(session, sessionProcessors)
                      case _ =>
                    }
                  case _ =>
                    session.status match {
                      case SessionStatus.ACTIVE =>
                        checkAndHandleEggpairActiveSession(session, sessionProcessors)
                      case SessionStatus.NEW =>
                        checkAndHandleEggpairOutTimeSession(session, sessionProcessors)
                      case _ =>
                    }
                }
              }
                catch {
                  case e: Throwable=>
                  logError(s"session watcher handle session ${session.id} error ${e.getMessage}")
                  e.printStackTrace()}

            }
            Thread.sleep(1000)
          }catch {
            case e: Throwable=>
                  logError(s"session watcher handle error ${e.getMessage}")
                 e.printStackTrace()
          }
        }
      }
    ,"SESSION_WATCHER_THREAD")
  }


  private val nodeHeartbeatChecker = new Thread(() => {
    var expire = CONFKEY_CLUSTER_MANAGER_NODE_HEARTBEAT_EXPIRED_COUNT.get().toInt * CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL.get().toInt
    while (true) {
      try {
        var now = System.currentTimeMillis();
        var nodes = ServerNodeCrudOperator.doGetServerNodes(ErServerNode(status = ServerNodeStatus.HEALTHY))
        nodes.foreach(n => {
          var interval = now - (if (n.lastHeartBeat != null) n.lastHeartBeat.getTime else now)
          // logInfo(s"interval : ${n.lastHeartBeat} ${interval}  ${now} ${if(n.lastHeartBeat!=null)n.lastHeartBeat.getTime}")
          if (interval > expire) {
            logInfo(s"server node ${n} change status to LOSS")
            updateNode(n.copy(status = ServerNodeStatus.LOSS), false, false)
          }
        })
      } catch {
        case e: Throwable =>
          logError(s"handle node heart beat error ${e.getMessage}")
          e.printStackTrace()
      }
      Thread.sleep(expire + 1000)
    }
  },"NODE_HEART_BEAT_CHECK_THREAD");

  def start(): Unit = {
    sessionWatcher.start()
    nodeHeartbeatChecker.start()
    //nodeProcessChecker.start()
  }

  def updateNode(serverNode: ErServerNode, needUpdateResource: Boolean, isHeartbeat: Boolean): ErServerNode = synchronized {
    serverNodeCrudOperator.updateServerNodeById(serverNode, isHeartbeat)
    if (needUpdateResource)
      registerResource(serverNode)
    serverNode
  }

  def queryNodeById(serverNode: ErServerNode): ErServerNode = {
    serverNodeCrudOperator.getServerNode(ErServerNode(id = serverNode.id))
  }

  def queryNodeByEndPoint(serverNode: ErServerNode): ErServerNode = {
    serverNodeCrudOperator.getServerNode(ErServerNode(endpoint = serverNode.endpoint))
  }


  def registerResource(data: ErServerNode): ErServerNode = synchronized {
    logInfo(s"node ${data.id} register resource ${data.resources.mkString}")
    var existResources = serverNodeCrudOperator.getNodeResources(data.id, "")
    var registedResources = data.resources;
    var updateResources = ArrayBuffer[ErResource]()
    var deleteResources = ArrayBuffer[ErResource]()
    var insertResources = ArrayBuffer[ErResource]()

    /**
     * 删掉多余资源，插入新增资源，修改已有资源
     */
    existResources.foreach(e => {
      var needUpdate = registedResources.filter(r => {
        r.resourceType == e.resourceType
      }).map(_.copy(allocated = -1))
      if (needUpdate.length > 0) {
        updateResources ++= needUpdate
      } else {
        deleteResources += e
      }
    })
    registedResources.foreach(r => {
      if (!updateResources.contains(r)) {
        insertResources += r
      }
    })
    ServerNodeCrudOperator.registerResource(data.id, insertResources.toArray, updateResources.toArray, deleteResources.toArray)
    data
  }

  def createNewNode(serverNode: ErServerNode): ErServerNode = synchronized {
    //    var  existNode =  serverNodeCrudOperator.getServerNode(serverNode.copy(status = ""))
    //    if(existNode==null){
    //      logInfo(s"create new node ${serverNode}")
    var existNode = serverNodeCrudOperator.createServerNode(serverNode);

    registerResource(serverNode.copy(id = existNode.id))
    //    }else{
    //      logInfo(s"node already exist ${existNode}")
    //    }


  }
}

class ClusterManagerService extends ClusterManager with Logging {

  import com.webank.eggroll.core.resourcemanager.ClusterManagerService._

  override def nodeHeartbeat(nodeHeartbeat: ErNodeHeartbeat): ErNodeHeartbeat = synchronized {
    //logInfo(s" nodeHeartbeat ${nodeHeartbeat}")
    var serverNode = nodeHeartbeat.node
    if (serverNode.id == -1) {
      var existNode = queryNodeByEndPoint(serverNode)
      if (existNode == null) {
        logInfo(s"create new node ${serverNode}")
        createNewNode(serverNode)
      } else {
        logInfo(s"node already exist ${existNode}")
        serverNode = serverNode.copy(id = existNode.id)
        updateNode(serverNode, true, true)
      }

    } else {
      if (nodeHeartbeatMap.contains(serverNode.id) && (nodeHeartbeatMap(serverNode.id).id < nodeHeartbeat.id)) {
        //正常心跳
        updateNode(serverNode, false, true)
      } else {
        //nodemanger重启过
        var existNode = queryNodeById(serverNode)
        if (existNode == null) {
          serverNode = createNewNode(serverNode)
        } else {
          updateNode(serverNode, true, true)
        }
      }
    }
    nodeHeartbeatMap.put(serverNode.id, nodeHeartbeat);
    nodeHeartbeat.copy(node = serverNode)
  }


}

