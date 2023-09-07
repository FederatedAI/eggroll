package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.deepspeed.job.meta.PrepareJobDownloadRequest
import com.webank.eggroll.core.ex.grpc.ExtendTransferClient
import com.webank.eggroll.core.meta.ErEndpoint
import com.webank.eggroll.core.transfer.Extend
import io.grpc.stub.StreamObserver
import org.junit.Test

import java.util.concurrent.CountDownLatch

class TestLogStream {

  @Test
  def testLogStream :Unit = {

    var latch :CountDownLatch = new CountDownLatch(1)
    val extendTransferClient = new ExtendTransferClient(ErEndpoint(host = "localhost", port = 4670))
    var requestSb =extendTransferClient.fetchLog(new StreamObserver[Extend.GetLogResponse] {

      override def onNext(v: Extend.GetLogResponse): Unit = {
        System.err.println("receive log "+v)
      }

      override def onError(throwable: Throwable): Unit = {
        System.err.println("receive log onError ")
        throwable.printStackTrace()
      }

      override def onCompleted(): Unit = {
        System.err.println("receive log onCompleted ")
      }
    })
    //deepspeed_session_20230705-175508-766715
    var request : Extend.GetLogRequest = Extend.GetLogRequest.newBuilder().setSessionId("deepspeed_session_20230705-175508-766715").setLogType("INFO").build();
    requestSb.onNext(request)
    Thread.sleep(10000)
    requestSb.onCompleted()

    latch.await()



  }


}
