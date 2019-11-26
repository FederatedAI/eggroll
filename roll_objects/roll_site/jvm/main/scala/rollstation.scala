package com.webank.ai.eggroll.rollstation

import com.webank.eggroll.rollsite.FdnCommunicationServer

//package com.king

//import io.grpc.stub.StreamObserver
//import java.util.concurrent.CountDownLatch


object rollstation {
  def main(args: Array[String]):Unit = {
    println("Hello World!xxx")
    val delegate = new FdnCommunicationServer
    delegate.greet()
  }
}

