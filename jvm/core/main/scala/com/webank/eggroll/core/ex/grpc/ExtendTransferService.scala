package com.webank.eggroll.core.ex.grpc


import com.webank.eggroll.core.client.NodeManagerClient
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

 class  NodeManagerExtendTransferService extends ExtendTransferServerGrpc.ExtendTransferServerImplBase with Logging{

    override def getLog(responseObserver: StreamObserver[Extend.GetLogResponse]): StreamObserver[Extend.GetLogRequest] = {
      var containersServiceHandler = ContainersServiceHandler.get()

      new  StreamObserver[Extend.GetLogRequest]{
        var  status :String = "init"
        var  logStreamHolder: LogStreamHolder = null

        override def onNext(request: Extend.GetLogRequest): Unit = {
          if(status=="init"){
            try {
              logInfo(s"receive get log request ${request}");
              logStreamHolder = containersServiceHandler.createLogStream(request, responseObserver)
              status = "running"
              logStreamHolder.run()
            }catch {
              case e:PathNotExistException =>
                  responseObserver.onNext(Extend.GetLogResponse.newBuilder().setCode("110").setMsg(e.getMessage).build())
                  responseObserver.onCompleted()
            }
          }
        }

        override def onError(throwable: Throwable): Unit = {
          System.err.println("receive on error")
          status = "error"
          if(logStreamHolder!=null) {
               logStreamHolder.stop()
          }
        }

        override def onCompleted(): Unit = {
          System.err.println("receive on stop")
          status = "stop"
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
      var  status :String = "init"
      var  requestSb :StreamObserver[Extend.GetLogRequest] =null

      override def onNext(request: Extend.GetLogRequest): Unit = {
        var  index = 0
        try {
          if (status == "init") {
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
            logInfo(s"prepare to send log request1 to  ${erServerNode.endpoint.host}  ${erServerNode.endpoint.port}");
            val extendTransferClient = new ExtendTransferClient(ErEndpoint(host = erServerNode.endpoint.host, port = erServerNode.endpoint.port))
            requestSb = extendTransferClient.fetchLog(responseObserver)
            status = "running"
          }
        }catch{
          case e: RankNotExistException =>{
            status ="error"
            responseObserver.onNext(Extend.GetLogResponse.newBuilder().setCode("111").setMsg(e.getMessage).build())
            responseObserver.onCompleted()
          }
        }


          if (status == "running" && requestSb != null) {

            requestSb.onNext(request.toBuilder.setRank(index.toString).build())
          }


      }

      override def onError(throwable: Throwable): Unit = {
        if(status =="running"&&requestSb!=null){
          requestSb.onError(throwable)
        }
      }

      override def onCompleted(): Unit = {
        if(status =="running"&&requestSb!=null){
          requestSb.onCompleted()
        }
      }
    }



  }



}

