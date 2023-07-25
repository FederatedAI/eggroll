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



//
//  def call(commandUri: CommandURI, args: Array[Byte]*): ByteString = {
//    logDebug(s"[CommandClient.call, single endpoint] commandUri: ${commandUri.uriString}, endpoint: ${defaultEndpoint}")
//    try {
//      val stub = CommandServiceGrpc.newBlockingStub(GrpcClientUtils.getChannel(defaultEndpoint))
//      val resp = stub.call(Command.CommandRequest.newBuilder
//        .setId(System.currentTimeMillis + "")
//        .setUri(commandUri.uri.toString)
//        .addAllArgs(args.map(ByteString.copyFrom).asJava)
//        .build)
//      resp.getResults(0)
//    } catch {
//      case t: Throwable =>
//        logError(s"[COMMAND] error calling to ${defaultEndpoint}, message: ${args(0)}. commandUri: ${commandUri.uriString}", t)
//        throw new CommandCallException(commandUri, defaultEndpoint, t)
//    }
//  }
//
//  def call[T](commandUri: CommandURI, args: Array[(Array[RpcMessage], ErEndpoint)])(implicit tag: ClassTag[T]): Array[T] = {
//    logDebug(s"[CommandClient.call] commandUri: ${commandUri.uriString}, endpoint: ${defaultEndpoint}")
//    val futures = args.map {
//      case (rpcMessages, endpoint) =>
//        try {
//          val ch: ManagedChannel = GrpcClientUtils.getChannel(endpoint)
//          val stub = CommandServiceGrpc.newFutureStub(ch)
//          val argBytes = rpcMessages.map(x => UnsafeByteOperations.unsafeWrap(SerdesUtils.rpcMessageToBytes(x, SerdesTypes.PROTOBUF)))
//          logDebug(s"[CommandClient.call, multiple endpoints] commandUri: ${commandUri.uriString}, endpoint: ${endpoint}")
//          stub.call(
//            Command.CommandRequest.newBuilder
//              .setId(s"${sessionId}-command-${TimeUtils.getNowMs()}")
//              .setUri(commandUri.uri.toString)
//              .addAllArgs(argBytes.toList.asJava).build)
//        } catch {
//          case t: Throwable =>
//            logError(s"[COMMAND] error calling to ${endpoint}. commandUri: ${commandUri.uriString}")
//            throw new CommandCallException(commandUri, endpoint, t)
//        }
//    }
//
//    futures.zipWithIndex.map{ case (f, n) =>
//      try{
//        SerdesUtils.rpcMessageFromBytes(f.get().getResults(0).toByteArray,
//          tag.runtimeClass, SerdesTypes.PROTOBUF).asInstanceOf[T]
//      } catch {
//        case t: Throwable =>
//          logError(s"[COMMAND] error calling to ${args(n)._2}. commandUri: ${commandUri.uriString}")
//          throw new CommandCallException(commandUri, args(n)._2, t)
//      }
//
//    }
  }

