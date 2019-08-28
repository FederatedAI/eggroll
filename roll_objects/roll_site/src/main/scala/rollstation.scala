package com.webank.ai.eggroll.rollstation

//package com.king

//import io.grpc.stub.StreamObserver
//import java.util.concurrent.CountDownLatch
import com.webank.ai.eggroll.rollsite.FdnCommunicationServer


object rollstation {
  def main(args: Array[String]):Unit = {
    println("Hello World!xxx")
    val delegate = new FdnCommunicationServer
    delegate.greet()
  }
}

