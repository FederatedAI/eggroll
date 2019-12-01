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
 *
 *
 */

package com.webank.eggroll.core.command

import java.lang.reflect.Method
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets

import com.google.protobuf.{ByteString, Message => PbMessage}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.datastructure.RpcMessage
import com.webank.eggroll.core.serdes._
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ArrayBuffer
import scala.collection.{immutable, mutable}

trait CommandRpcMessage extends RpcMessage {
  override def rpcMessageType(): String = "Command"
}

case class ErService(serviceName: String,
                     serviceParamTypes: Array[Class[_]],
                     serviceResultTypes: Array[Class[_]],
                     serviceParamDeserializers: Array[ErDeserializer],
                     serviceResultSerializers: Array[ErSerializer],
                     routeToMethod: Method,
                     callBasedInstance: Any,
                     scope: String = StringConstants.EMPTY)

case class ErCommandRequest(id: String = System.currentTimeMillis().toString, uri: String, args: Array[Array[Byte]] = null, kwargs: immutable.Map[String, Array[Byte]] = null) extends CommandRpcMessage

case class ErCommandResponse(id: String, request: ErCommandRequest = null, results: Array[Array[Byte]] = null) extends CommandRpcMessage

class CommandURI(val uriString: String) {
  val uri = new URI(uriString)
  val queryString = uri.getQuery
  private val queryPairs = mutable.Map[String, String]()

  def this(src: ErCommandRequest) {
    this(src.uri)
  }

  def this(prefix: String, name: String) {
    this(s"${prefix}/${name}")
  }

  def getName(): String = {
    StringUtils.substringAfterLast(uri.getPath, StringConstants.SLASH)
  }

  /*  def this(src: ErCommandResponse) {
      this(src.request.uri)
    }*/

  if (StringUtils.isBlank(queryString)) {
    queryPairs.put(StringConstants.ROUTE, uriString)
  } else {
    for (pair <- queryString.split(StringConstants.AND)) {
      val idx = pair.indexOf(StringConstants.EQUAL)
      val key = if (idx > 0) URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8.name()) else pair
      val value = if (idx > 0 && pair.length > idx + 1) URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8.name()) else StringConstants.EMPTY
      queryPairs.put(key, value)
    }
  }

  def getQueryValue(key: String): String = {
    queryPairs(key)
  }

  def getRoute(): String = {
    queryPairs(StringConstants.ROUTE)
  }
}

object CommandModelPbMessageSerdes {

  implicit class ErCommandRequestToPbMessage(src: ErCommandRequest) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Command.CommandRequest = {
      val builder = Command.CommandRequest.newBuilder()
        .setId(src.id)
        .setUri(src.uri)

      if (src.args != null) {
        src.args.foreach(arg => builder.addArgs(ByteString.copyFrom(arg)))
      }
      if (src.kwargs != null) {
        src.kwargs.foreach(kwarg => builder.putKwargs(kwarg._1, ByteString.copyFrom(kwarg._2)))
      }
      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErCommandRequest].toBytes()
  }

  implicit class ErCommandResponseToPbMessage(src: ErCommandResponse) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Command.CommandResponse = {
      val builder = Command.CommandResponse.newBuilder()
        .setId(src.id)
        .setRequest(if (src.request == null) Command.CommandRequest.getDefaultInstance else src.request.toProto())

      if (src.results != null) {
        src.results.foreach(d => builder.addResults(ByteString.copyFrom(d)))
      }

      builder.build()
    }

    override def toBytes(baseSerializable: BaseSerializable): Array[Byte] =
      baseSerializable.asInstanceOf[ErCommandResponse].toBytes()
  }

  implicit class ErCommandRequestFromPbMessage(src: Command.CommandRequest) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErCommandRequest = {
      val args = ArrayBuffer[Array[Byte]]()
      val argsCount = src.getArgsCount
      if (argsCount > 0) {
        src.getArgsList.forEach(bs => args.append(bs.toByteArray))
      }

      val kwargs = mutable.Map[String, Array[Byte]]()
      if (src.getKwargsCount > 0) {

        src.getKwargsMap.entrySet().forEach(entry => kwargs.put(entry.getKey, entry.getValue.toByteArray))
      }

      ErCommandRequest(id = src.getId,
        uri = src.getUri,
        args = args.toArray,
        kwargs = kwargs.toMap)
    }

    override def fromBytes(bytes: Array[Byte]): ErCommandRequest =
      Command.CommandRequest.parseFrom(bytes).fromProto()
  }

  implicit class ErCommandResponseFromPbMessage(src: Command.CommandResponse) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErCommandResponse = {
      val results = ArrayBuffer[Array[Byte]]()
      if (src.getResultsCount > 0) {
        src.getResultsList.forEach(r => results.append(r.toByteArray))
      }
      ErCommandResponse(
        id = src.getId,
        request = src.getRequest.fromProto(),
        results = results.toArray)
    }

    override def fromBytes(bytes: Array[Byte]): ErCommandResponse =
      Command.CommandResponse.parseFrom(bytes).fromProto()
  }
}
