package com.webank.eggroll.core.ex.grpc


import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.constant.LogStreamStatus.{ERROR_STATUS, INIT_STATUS, PREPARE_STATUS, RUNNING_STATUS, STOP_STATUS}
import com.webank.eggroll.core.containers.ContainersServiceHandler
import com.webank.eggroll.core.containers.ContainersServiceHandler.LogStreamHolder
import com.webank.eggroll.core.deepspeed.job.JobServiceHandler.smDao
import com.webank.eggroll.core.error.{ErSessionException, PathNotExistException, RankNotExistException}
import com.webank.eggroll.core.meta.{ErEndpoint, ErServerNode}
import com.webank.eggroll.core.resourcemanager.BaseDao.NotExistError
import com.webank.eggroll.core.resourcemanager.SessionMetaDao
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator
import com.webank.eggroll.core.transfer.{Extend, ExtendTransferServerGrpc}
import com.webank.eggroll.core.util.{Logging, ProcessUtils}
import io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import scala.util.control.Breaks.{break, breakable}

 class  NodeManagerExtendTransferService extends ExtendTransferServerGrpc.ExtendTransferServerImplBase with Logging{
//    val  INIT_STATUS:String="init"
//    val  PREPARE_STATUS:String="prepare"
//    val  RUNNING_STATUS:String="running"
//    val  ERROR_STATUS:String="error"
//    val STOP_STATUS:String ="stop"


    override def getLog(responseObserver: StreamObserver[Extend.GetLogResponse]): StreamObserver[Extend.GetLogRequest] = {
      var containersServiceHandler = ContainersServiceHandler.get()

      new  StreamObserver[Extend.GetLogRequest]{
        var  status :String = INIT_STATUS
        var  logStreamHolder: LogStreamHolder = null
        var  instanceId =  UUID.randomUUID();


        override    def onNext(request: Extend.GetLogRequest): Unit = synchronized{

            logInfo(s"instance ${instanceId} receive get log request for ${request.getSessionId} ${request.getLogType}");
            if(status==INIT_STATUS) {

              status = PREPARE_STATUS
              var thread = new Thread(new Runnable {
                override def run(): Unit = {
                    breakable {
                      var tryCount = 0
                      while (tryCount < 20 && status == PREPARE_STATUS) {
                        tryCount = tryCount + 1;
                        try {
                          logInfo(s"try to create log stream holder ${request.getSessionId} try count ${tryCount}")
                          logStreamHolder = containersServiceHandler.createLogStream(request, responseObserver)
                          if (status == PREPARE_STATUS) {
                            logStreamHolder.run()
                            status = RUNNING_STATUS
                          } else {
                            responseObserver.onCompleted()
                          }
                          break()
                        } catch {
                          case e: PathNotExistException =>
                            Thread.sleep(5000)
                            logInfo(s"path not found for ${e.getMessage}")
                        }
                      }
                    }
                    if (logStreamHolder == null) {

                      responseObserver.onNext(Extend.GetLogResponse.newBuilder().setCode("110").setMsg("log file is not found").build())
                      responseObserver.onCompleted()
                    }

                }
              })
              thread.start()
            }

        }

        override def onError(throwable: Throwable): Unit = {
          status = ERROR_STATUS
          logInfo(s"instance ${instanceId} receive onError");
          if(logStreamHolder!=null) {
               logStreamHolder.stop()
          }
        }

        override def onCompleted(): Unit = {
          logInfo(s"instance ${instanceId} receive onCompleted");
          status = STOP_STATUS
          if(logStreamHolder!=null){
            logStreamHolder.stop()
          }
        }
      }
    }

}


class ClusterManagerExtendTransferService  extends   ExtendTransferServerGrpc.ExtendTransferServerImplBase with Logging{

  private lazy val smDao = new SessionMetaDao
  private lazy val nodeDao = new  ServerNodeCrudOperator;

  override def getLog( responseObserver: StreamObserver[Extend.GetLogResponse]): StreamObserver[Extend.GetLogRequest] = {

    new  StreamObserver[Extend.GetLogRequest]{
      var  status :String = INIT_STATUS
      var  requestSb :StreamObserver[Extend.GetLogRequest] =null

      override def onNext(request: Extend.GetLogRequest): Unit = {
        var  index = 0
        try {
          if (status == INIT_STATUS) {
              status==PREPARE_STATUS
            logInfo(s"receive get log request ${request}");
            if (StringUtils.isNotEmpty(request.getRank) && request.getRank.toInt > 0) {
              index = request.getRank.toInt
            }
            var rankInfos = smDao.getRanks(request.getSessionId)
            if (rankInfos == null || rankInfos.size == 0) {
              logError(s"can not found rank info for session ${request.getSessionId}")
              throw new RankNotExistException(s"can not found rank info for session ${request.getSessionId}")
            }
            val nodeIdMeta = rankInfos.filter(_._4 == index)
            if (nodeIdMeta == null || nodeIdMeta.size == 0) {
              logError(s"can not found rank info for session ${request.getSessionId} rank ${index}")
              throw new RankNotExistException(s"can not found rank info for session ${request.getSessionId} rank ${index}")
            }
            var rankInfo = nodeIdMeta.apply(0)
            logInfo(s"rank info ${rankInfo}")
            val erServerNode = nodeDao.getServerNode(ErServerNode(id = rankInfo._2))
            logInfo(s"prepare to send log request to  ${erServerNode.endpoint.host}  ${erServerNode.endpoint.port}");
            val extendTransferClient = new ExtendTransferClient(ErEndpoint(host = erServerNode.endpoint.host, port = erServerNode.endpoint.port))
            requestSb = extendTransferClient.fetchLog(responseObserver)
            status = RUNNING_STATUS
          }
        }catch{
          case e: RankNotExistException =>{
            status =ERROR_STATUS
            responseObserver.onNext(Extend.GetLogResponse.newBuilder().setCode("111").setMsg(e.getMessage).build())
            responseObserver.onCompleted()
          }
        }
          if (status == RUNNING_STATUS && requestSb != null) {

            requestSb.onNext(request.toBuilder.setRank(index.toString).build())
          }
      }

      override def onError(throwable: Throwable): Unit = {
        if(status ==RUNNING_STATUS&&requestSb!=null){
          requestSb.onError(throwable)
        }
      }

      override def onCompleted(): Unit = {
        if(status ==RUNNING_STATUS&&requestSb!=null){
          requestSb.onCompleted()
        }
      }
    }

  }



}

