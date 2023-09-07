package com.webank.eggroll.core.ex.grpc

import com.google.protobuf.{ByteString, UnsafeByteOperations}
import com.webank.eggroll.core.command.{Command, CommandResponseObserver, CommandServiceGrpc, CommandURI}
import com.webank.eggroll.core.constant.{SerdesTypes, SessionConfKeys}
import com.webank.eggroll.core.datastructure.RpcMessage
import com.webank.eggroll.core.error.CommandCallException
import com.webank.eggroll.core.meta.ErEndpoint
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.transfer.{Extend, ExtendTransferServerGrpc, GrpcClientUtils}
import com.webank.eggroll.core.util.{Logging, SerdesUtils, TimeUtils}
import io.grpc.stub.StreamObserver
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.collection.concurrent.TrieMap
import scala.reflect.ClassTag


object ExtendTransferClient {
  val pool = TrieMap[String, ManagedChannel]()
}


class ExtendTransferClient(defaultEndpoint: ErEndpoint = null,
                    serdesType: String = SerdesTypes.PROTOBUF,
                    isSecure:Boolean=false)
  extends Logging {
  // TODO:1: for java
  def this(){
    this(null, SerdesTypes.PROTOBUF, false)
  }


  val sessionId = StaticErConf.getString(SessionConfKeys.CONFKEY_SESSION_ID)

  // TODO:1: confirm client won't exit bug of Singletons.getNoCheck(classOf[GrpcChannelFactory]).getChannel(endpoint, isSecure)
  // TODO:0: add channel args
  // TODO:0: universally caches channels
  private def buildChannel(endpoint: ErEndpoint): ManagedChannel = synchronized {
    val channel = ExtendTransferClient.pool.getOrElse(endpoint.toString, null)
    if(channel == null || channel.isShutdown || channel.isTerminated) {
      val builder = ManagedChannelBuilder.forAddress(endpoint.host, endpoint.port)
      builder.maxInboundMetadataSize(1024*1024*1024)
      builder.maxInboundMessageSize(1024*1024*1024)
      builder.usePlaintext()
      ExtendTransferClient.pool(endpoint.toString) = builder.build()
    }
    ExtendTransferClient.pool(endpoint.toString)
  }

   def  fetchLog(responseObserver: StreamObserver[Extend.GetLogResponse]): StreamObserver[Extend.GetLogRequest] ={
    val stub = ExtendTransferServerGrpc.newStub(GrpcClientUtils.getChannel(defaultEndpoint))
    stub.getLog(responseObserver)
  }

  }

