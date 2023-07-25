package com.webank.eggroll.core.ex.grpc


import com.webank.eggroll.core.client.NodeManagerClient
import com.webank.eggroll.core.containers.ContainersServiceHandler
import com.webank.eggroll.core.containers.ContainersServiceHandler.LogStreamHolder
import com.webank.eggroll.core.deepspeed.job.JobServiceHandler.smDao
import com.webank.eggroll.core.meta.{ErEndpoint, ErServerNode}
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
            logInfo(s"receive get log request ${request}");
            logStreamHolder = containersServiceHandler.startLogStream(request,responseObserver)
            status="running"
          }
        }

        override def onError(throwable: Throwable): Unit = {
          logStreamHolder.stop()
        }

        override def onCompleted(): Unit = {
          logStreamHolder.stop()
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
        if(status=="init"){
          logInfo(s"receive get log request ${request}");
          var  index = 0
          if(StringUtils.isNotEmpty(request.getRank)&& request.getRank.toInt>0){
            index = request.getRank.toInt
          }
          val nodeIdMeta = smDao.getRanks(request.getSessionId).filter(_._4==index)
          var rankInfo = nodeIdMeta.apply(0)
          logInfo(s"rank info ${rankInfo}")
          val erServerNode = nodeDao.getServerNode( ErServerNode(id=rankInfo._2))
          logInfo(s"prepare to send log request1 to  ${erServerNode.endpoint.host}  ${erServerNode.endpoint.port}");
          val extendTransferClient = new ExtendTransferClient(ErEndpoint(host = erServerNode.endpoint.host, port = erServerNode.endpoint.port))
          requestSb =extendTransferClient.fetchLog(responseObserver)


          status="running"
        }
        if(status =="running"&&requestSb!=null){
          requestSb.onNext(request)
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

