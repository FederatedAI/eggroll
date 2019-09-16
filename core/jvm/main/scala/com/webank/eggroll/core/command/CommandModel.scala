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

package com.webank.eggroll.core.command

import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets

import com.google.protobuf.{ByteString, Message => PbMessage}
import com.webank.eggroll.core.constant.{ObjectConstants, StringConstants}
import com.webank.eggroll.core.rpc.RpcMessage
import com.webank.eggroll.core.serdes.{PbMessageDeserializer, PbMessageSerializer}
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

case class ErCommandRequest(seq: Long = System.currentTimeMillis(), uri: String, args: Array[Array[Byte]] = null, kwargs: mutable.Map[String, Array[Byte]] = null) extends RpcMessage

case class ErCommandResponse(seq: Long, request: ErCommandRequest = null, data: Array[Byte] = null) extends RpcMessage

class CommandURI(uriString: String) {
  val uri = new URI(uriString)
  val queryString = uri.getQuery
  private val queryPairs = mutable.Map[String, String]()

  def this(src: ErCommandRequest) {
    this(src.uri)
  }

  def this(src: ErCommandResponse) {
    this(src.request.uri)
  }

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

object CommandPbSerdes {

  implicit class ErCommandRequestToPbMessage(src: ErCommandRequest) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Command.CommandRequest = {
      val builder = Command.CommandRequest.newBuilder()
        .setSeq(src.seq)
        .setUri(src.uri)

      if (src.args != null) {
        src.args.map(arg => builder.addArgs(ByteString.copyFrom(arg)))
      }
      if (src.kwargs != null) {
        src.kwargs.map(kwarg => builder.putKwargs(kwarg._1, ByteString.copyFrom(kwarg._2)))
      }
      builder.build()
    }
  }

  implicit class ErCommandResponseToPbMessage(src: ErCommandResponse) extends PbMessageSerializer {
    override def toProto[T >: PbMessage](): Command.CommandResponse = {
      val builder = Command.CommandResponse.newBuilder()
        .setSeq(src.seq)
        .setRequest(if (src.request == null) Command.CommandRequest.getDefaultInstance else src.request.toProto())
        .setData(ByteString.copyFrom(src.data))

      builder.build()
    }
  }

  implicit class ErCommandRequestFromPbMessage(src: Command.CommandRequest) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErCommandRequest = {
      var args = ObjectConstants.EMPTY_ARRAY_OF_BYTE_ARRAY
      val argsCount = src.getArgsCount
      if (argsCount > 0) {
        val argsBuffer = new ArrayBuffer[Array[Byte]]()

        src.getArgsList.forEach(bs => argsBuffer.append(bs.toByteArray))

        args = argsBuffer.toArray
      }

      var kwargs = ObjectConstants.EMPTY_MUTABLE_MAP_OF_STRING_TO_BYTE_ARRAY
      if (src.getKwargsCount > 0) {
        kwargs = mutable.Map[String, Array[Byte]]()

        src.getKwargsMap.entrySet().forEach(entry => kwargs.put(entry.getKey, entry.getValue.toByteArray))
      }

      ErCommandRequest(seq = src.getSeq,
        uri = src.getUri,
        args = args,
        kwargs = kwargs)
    }

    override def fromBytes[T: ClassTag](bytes: Array[Byte]): T = ???
  }

  implicit class ErCommandResponseFromPbMessage(src: Command.CommandResponse) extends PbMessageDeserializer {
    override def fromProto[T >: RpcMessage](): ErCommandResponse = ErCommandResponse(
      seq = src.getSeq,
      request = src.getRequest.fromProto(),
      data = if (src.getData != null) src.getData.toByteArray else Array.emptyByteArray)

    override def fromBytes[T: ClassTag](bytes: Array[Byte]): T = ???
  }
}
