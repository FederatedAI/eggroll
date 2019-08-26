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
 */

package com.webank.eggroll.command

import java.io.{PrintWriter, StringWriter}
import java.lang.reflect.InvocationTargetException
import java.net.URI
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.google.protobuf.ByteString
import com.webank.eggroll.rollframe.{CommandServiceGrpc, RollFrameGrpc, ServerNode}
import com.webank.eggroll.util.ThrowableCollection
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver

import scala.collection.mutable

class CommandService {

}


class CommandURI(value: String) {
  import java.net.URLDecoder
  val uri = new URI(value)

  val queryPairs = mutable.Map[String, String]()
  for (pair <- uri.getQuery.split("&")) {
    val idx = pair.indexOf("=")
    val key = if (idx > 0) URLDecoder.decode(pair.substring(0, idx), "UTF-8") else pair
    val value = if (idx > 0 && pair.length > idx + 1) URLDecoder.decode(pair.substring(idx + 1), "UTF-8") else ""
    queryPairs.put(key, value)
  }

  def getQueryValue(key: String):String = {
    queryPairs(key)
  }
}
class GrpcCommandService extends CommandServiceGrpc.CommandServiceImplBase{
  override def call(request: RollFrameGrpc.CommandRequest,
                    responseObserver: StreamObserver[RollFrameGrpc.CommandResponse]): Unit = {
    val url = new CommandURI(request.getUri)
    val clazz = Class.forName(url.getQueryValue("class"))
    val methodName = url.getQueryValue("method")
    val service = clazz.newInstance()
    try{
      val resp = clazz.getMethod(methodName, classOf[Array[Byte]])
        .invoke(service, request.getData.toByteArray).asInstanceOf[Array[Byte]]
      responseObserver.onNext(
        RollFrameGrpc.CommandResponse.newBuilder().setRequest(request).setData(ByteString.copyFrom(resp)).build())
      responseObserver.onCompleted()
    } catch {
      case t: InvocationTargetException =>
        t.printStackTrace()
        responseObserver.onError(t.getTargetException)
    }
  }
}

case class EndpointCommand(uri: URI, req: Array[Byte], resp: Array[Byte])



// TODO: short timeout
class CollectiveCommand(nodes: List[ServerNode], timeout:Int = 600*1000) {
  private val clients = nodes.map{ node =>
    CommandServiceGrpc.newStub(
      ManagedChannelBuilder.forAddress(node.host, node.port).usePlaintext().build())
  }
  def syncSendAll(cmds: List[EndpointCommand]):List[EndpointCommand] = {
    val countDownLatch = new CountDownLatch(nodes.size)
    val errors = new ThrowableCollection()
    val resps = mutable.ListBuffer[EndpointCommand]()
    // TODO: zip -> join
    clients.zip(cmds).foreach{ case (client,cmd) =>
      val req = RollFrameGrpc.CommandRequest.newBuilder().
        setUri(cmd.uri.toString)
        .setData(ByteString.copyFrom(cmd.req))
        .build()
      client.call(req, new StreamObserver[RollFrameGrpc.CommandResponse] {
        override def onNext(value: RollFrameGrpc.CommandResponse): Unit = {
          resps.append(EndpointCommand(null, null, value.getData.toByteArray))
        }

        override def onError(t: Throwable): Unit = errors.append(t)

        override def onCompleted(): Unit = countDownLatch.countDown()
      })
    }
    try{
      countDownLatch.await(timeout, TimeUnit.MILLISECONDS)
      if(countDownLatch.getCount > 0) {
        throw new Exception("time out:" + timeout)
      }
    } catch {
      case e:Throwable => errors.append(e)
    } finally {
      errors.check()
    }
    resps.toList
  }
}