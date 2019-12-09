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

package com.webank.eggroll.core.util

import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.core.constant.SerdesTypes
import com.webank.eggroll.core.datastructure.RpcMessage
import com.webank.eggroll.core.serdes.{ErDeserializer, ErSerializer, RpcMessageSerdesFactory}

object SerdesUtils {
  private val serializerCache = new ConcurrentHashMap[String, ErSerializer]()
  private val deserializerCache = new ConcurrentHashMap[String, ErDeserializer]()

  def rpcMessageToBytes(rpcMessage: RpcMessage, serdesTypes: String = SerdesTypes.PROTOBUF): Array[Byte] = {
    assert(rpcMessage != null)

    val javaClass = rpcMessage.getClass
    val javaClassName = javaClass.getCanonicalName

    if (!serializerCache.contains(javaClassName)) {
      val newSerializer = RpcMessageSerdesFactory.newSerializer(javaClass, serdesTypes)
      serializerCache.putIfAbsent(javaClassName, newSerializer)
    }

    val serializer = serializerCache.get(javaClassName)

    serializer.toBytes(rpcMessage)
  }

  def rpcMessageFromBytes[T >: RpcMessage](bytes: Array[Byte], targetType: Class[_], serdesTypes: String = SerdesTypes.PROTOBUF): T = {
    assert(bytes != null)

    val javaClassName = targetType.getCanonicalName

    if (!deserializerCache.contains(javaClassName)) {
      val newDeserializer = RpcMessageSerdesFactory.newDeserializer(targetType, serdesTypes)
      deserializerCache.putIfAbsent(javaClassName, newDeserializer)
    }

    val deserializer = deserializerCache.get(javaClassName)

    deserializer.fromBytes(bytes).asInstanceOf[T]
  }
}
