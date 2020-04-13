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

package com.webank.eggroll.core.serdes

import java.lang.reflect.Constructor
import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.core.command.CommandRpcMessage
import com.webank.eggroll.core.constant.{SerdesTypes, StringConstants}
import com.webank.eggroll.core.datastructure.SerdesFactory
import com.webank.eggroll.core.meta.{MetaRpcMessage, NetworkingRpcMessage, TransferRpcMessage}
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils

object RpcMessageSerdesFactory extends SerdesFactory with Logging {
  private val serdesTypeToNames = Map((SerdesTypes.PROTOBUF -> "PbMessage"))
  private val serdesToConstructors = new ConcurrentHashMap[String, Constructor[_]]()
  private val DIRECTION_SERIALIZE = "To"
  private val DIRECTION_DESERIALIZE = "From"

  def newSerializer(javaClass: Class[_], serdesType: String = SerdesTypes.PROTOBUF): ErSerializer = {
    newSerdesInternal(
      javaClass = javaClass, serdesType = serdesType, direction = DIRECTION_SERIALIZE)
      .asInstanceOf[ErSerializer]
  }

  def newDeserializer(javaClass: Class[_], serdesType: String = SerdesTypes.PROTOBUF): ErDeserializer = {
    newSerdesInternal(
      javaClass = javaClass, serdesType = serdesType, direction = DIRECTION_DESERIALIZE)
      .asInstanceOf[ErDeserializer]
  }

  private def newSerdesInternal(javaClass: Class[_], serdesType: String, direction: String): Any = {
    var finalJavaClass = javaClass
    val javaClassCanonicalName = javaClass.getCanonicalName

    var cacheKey = genCacheKey(
      className = javaClassCanonicalName, serdesType = serdesType, direction = direction)
    var constructor = serdesToConstructors.get(cacheKey)

    // return from cache if exists
    if (constructor != null) {
      return constructor.newInstance(null)
    }

    // create and put to cache otherwise
    val endsWithDollarSign = javaClassCanonicalName.endsWith(StringConstants.DOLLAR)
    val strippedJavaClassName = StringUtils.strip(javaClassCanonicalName, StringConstants.DOLLAR)

    if (endsWithDollarSign) {
      finalJavaClass = Class.forName(strippedJavaClassName)
    }

    var rpcMessageType: String = null
    if (classOf[MetaRpcMessage].isAssignableFrom(finalJavaClass)) {
      rpcMessageType = "Meta"
    } else if (classOf[CommandRpcMessage].isAssignableFrom(finalJavaClass)) {
      rpcMessageType = "Command"
    } else if (classOf[TransferRpcMessage].isAssignableFrom(finalJavaClass)) {
      rpcMessageType = "Transfer"
    } else if (classOf[NetworkingRpcMessage].isAssignableFrom(finalJavaClass)) {
      rpcMessageType = "Networking"
    }

    val serdesName = serdesTypeToNames(serdesType)
    val scalaClassName =
      s"com.webank.eggroll.core.meta.${rpcMessageType}Model${serdesName}Serdes" +
        s"$$${finalJavaClass.getSimpleName}${direction}${serdesName}"

    val clz = Class.forName(scalaClassName)

    constructor = clz.getConstructors()(0)

    cacheKey = genCacheKey(
      className = strippedJavaClassName, serdesType = serdesType, direction = direction)
    serdesToConstructors.putIfAbsent(cacheKey, constructor)
    serdesToConstructors.putIfAbsent(cacheKey + StringConstants.DOLLAR, constructor)

    constructor.newInstance(null)
  }

  private def genCacheKey(className: String, serdesType: String, direction: String): String = {
    String.join(StringConstants.UNDERLINE, direction, serdesType, className)
  }
}
