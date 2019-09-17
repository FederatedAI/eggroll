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

import java.lang.reflect.{InvocationTargetException, Method}
import java.net.URI
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.google.protobuf.{ByteString, Message}
import com.webank.eggroll.rollframe.{CommandServiceGrpc, RollFrameGrpc, ServerNode}
import com.webank.eggroll.util.ThrowableCollection
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class CommandService {

}

// TODO: organize service definitions
object CommandService {
  private val routes = TrieMap[String, (Any, Method)]()
  def register(route:String, args:List[Class[_]], ret:Class[_],
               clazz:Class[_] = null, method:String = null, service:Any = null):Unit =
    this.synchronized {
    if(routes.contains(route)) {
      throw new IllegalStateException("route has been registered:" + route + "," + routes(route))
    }
    val methodNameIndex = route.lastIndexOf(".")
    val methodName =  if(method != null) method else route.substring(methodNameIndex + 1)
    val clz = if(clazz != null) clazz else Class.forName(route.substring(0, methodNameIndex))
    val methodInstance = clz.getMethod(methodName, args:_*)
    routes.put(route, (if(service != null) service else clz, methodInstance))
  }

  def dispatch(route:String, args:AnyRef*):AnyRef = {
    val (service, method) = routes(route)
    val obj = service match {
      case clazz: Class[_] => clazz.newInstance()
      case _ => service
    }
    method.invoke(obj, args:_*)
  }

  def query(route: String):(Any, Method) = routes(route)
}


class CommandURI(value: String) {
  import java.net.URLDecoder
  val uri = new URI(value)

  private val queryPairs = mutable.Map[String, String]()
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

trait CommandServer {
  def run():Unit
}

class GrpcCommandService extends CommandServiceGrpc.CommandServiceImplBase{
  override def call(request: RollFrameGrpc.CommandRequest,
                    responseObserver: StreamObserver[RollFrameGrpc.CommandResponse]): Unit = {
    val route = new CommandURI(request.getUri).getQueryValue("route")
    val (_, method) = CommandService.query(route)
    try{
      if(classOf[Message].isAssignableFrom(method.getReturnType)) {
        require(method.getParameterTypes.length == 1, "only one parameter supported")
        val req = method.getParameterTypes.head.getMethod("parseFrom", classOf[ByteString])
                      .invoke(null,request.getData)
        val resp = CommandService.dispatch(route, req)
        // Should set request back?
        responseObserver.onNext(
          RollFrameGrpc.CommandResponse.newBuilder().setRequest(request)
            .setData(resp.getClass.getMethod("toByteString").invoke(resp).asInstanceOf[ByteString]).build())
        responseObserver.onCompleted()
      } else {
        throw new IllegalArgumentException("not supported resp type:" + method.getReturnType)
      }
    } catch {
      case t: InvocationTargetException =>
        t.printStackTrace()
        responseObserver.onError(t.getTargetException)
    }
  }
}

case class EndpointCommand(uri: URI, req: Array[Byte], resp: Array[Byte], serverNode: ServerNode = null)

// TODO: short timeout
class CollectiveCommand(nodes: List[ServerNode], timeout:Int = 600*1000) {
  private val clients = nodes.map{ node =>
    (node, CommandServiceGrpc.newStub(
      ManagedChannelBuilder.forAddress(node.host, node.port).usePlaintext().build()))
  }.toMap
  def syncSendAll(cmds: List[EndpointCommand]):List[EndpointCommand] = {
    val countDownLatch = new CountDownLatch(cmds.size)
    val errors = new ThrowableCollection()
    val resps = mutable.ListBuffer[EndpointCommand]()
    cmds.foreach{ cmd =>
      val req = RollFrameGrpc.CommandRequest.newBuilder().
        setUri(cmd.uri.toString)
        .setData(ByteString.copyFrom(cmd.req))
        .build()
      clients(cmd.serverNode).call(req, new StreamObserver[RollFrameGrpc.CommandResponse] {
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