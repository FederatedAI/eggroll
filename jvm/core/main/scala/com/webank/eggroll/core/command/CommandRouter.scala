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

import com.google.protobuf.ByteString
import com.webank.eggroll.core.constant.{CoreConfKeys, SerdesTypes, StringConstants}
import com.webank.eggroll.core.error.CommandRoutingException
import com.webank.eggroll.core.serdes.{BaseSerializable, ErDeserializer, ErSerializer, RpcMessageSerdesFactory}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.reflect.{ConstructorUtils, MethodUtils}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait CommandRoutable {

}

object Scope {

}

object CommandRouter extends Logging {
  private val defaultSerdesType = StaticErConf.getString(CoreConfKeys.CONFKEY_CORE_COMMAND_DEFAULT_SERDES_TYPE, SerdesTypes.PROTOBUF)

  private val handlers = TrieMap[String, (Array[ByteString] => Array[Byte])]()

  // todo:2: consider different scope of target instance such as 'singleton', 'proto', 'session' etc.
  //  This can be implemented as an annotation reader
  def register(serviceName: String,
               serviceParamTypes: Array[Class[_]],
               serviceResultTypes: Array[Class[_]] = Array(),
               serviceParamDeserializers: Array[String] = Array(),
               serviceResultSerializers: Array[String] = Array(),
               routeToClass: Class[_] = null,
               routeToMethodName: String = null,
               routeToCallBasedClassInstance: Any = null,
               callBasedClassInstanceInitArgs: Array[AnyRef] = null): Unit = this.synchronized {
    val finalServiceName = if (serviceName.startsWith("/v2/")) {
      val toks = serviceName.replaceFirst("/v2/", "com/webank/").split("/")
      toks(toks.length - 2) += "Service"
      toks.mkString(".")
    } else {
      serviceName
    }
    val finalRouteToMethodName =
      if (routeToMethodName != null) routeToMethodName
      else StringUtils.substringAfterLast(finalServiceName, StringConstants.DOT)

    val finalRouteToClass =
      if (routeToClass != null) routeToClass
      else Class.forName(StringUtils.substringBeforeLast(finalServiceName, StringConstants.DOT))

    val routeToMethod = if (serviceParamTypes == null)
      finalRouteToClass.getMethods.find(_.getName == finalRouteToMethodName).get
    else MethodUtils.getAccessibleMethod(
      finalRouteToClass, finalRouteToMethodName, serviceParamTypes: _*)

    if (routeToMethod == null) {
      throw new NoSuchMethodException(s"accessible method ${routeToMethod} not found for ${finalServiceName}")
    }

    val finaleServiceParamTypes = routeToMethod.getParameterTypes
    val finalServiceResultTypes: Array[Class[_]] =
      if (serviceResultTypes.isEmpty) Array(routeToMethod.getReturnType) else serviceResultTypes

    val finalCallBasedInstance =
      if (routeToCallBasedClassInstance != null) {
        routeToCallBasedClassInstance
      } else {
        val finalCallBasedClassInstanceInitArgsArray =
          if (callBasedClassInstanceInitArgs != null) callBasedClassInstanceInitArgs.toArray else null
        ConstructorUtils.invokeConstructor(finalRouteToClass, finalCallBasedClassInstanceInitArgsArray: _*)
      }

    val paramDeserializers = ArrayBuffer[ErDeserializer]()
    paramDeserializers.sizeHint(finaleServiceParamTypes.length)
    finaleServiceParamTypes.indices.foreach(i => {
      val serdesType =
        if (serviceParamDeserializers.length - 1 < i) defaultSerdesType else serviceParamDeserializers(i)
      val deserializer = RpcMessageSerdesFactory.newDeserializer(
        javaClass = finaleServiceParamTypes(i), serdesType = serdesType)
      paramDeserializers += deserializer
    })

    val resultSerializers = ArrayBuffer[ErSerializer]()
    resultSerializers.sizeHint(finalServiceResultTypes.length)
    finalServiceResultTypes.indices.foreach(i => {
      val serdesType =
        if (serviceResultSerializers.length - 1 < i) defaultSerdesType else serviceResultSerializers(i)
      val serializer = RpcMessageSerdesFactory.newSerializer(
        javaClass = finalServiceResultTypes(i), serdesType = serdesType)
      resultSerializers += serializer
    })

    val command = ErService(
      serviceName = finalServiceName,
      serviceParamTypes = finaleServiceParamTypes,
      serviceResultTypes = finalServiceResultTypes,
      serviceParamDeserializers = paramDeserializers.toArray,
      serviceResultSerializers = resultSerializers.toArray,
      callBasedInstance = finalCallBasedInstance,
      routeToMethod = routeToMethod)

    register_handler(serviceName, (args: Array[ByteString]) => {
      val realArgs = args.zip(command.serviceParamDeserializers).map {
        case (arg, deserializer) => {
          deserializer.fromBytes(arg.asInstanceOf[ByteString].toByteArray).asInstanceOf[Object]
        }
      }
      val callResult = command.routeToMethod.invoke(command.callBasedInstance, realArgs: _*) // method.invoke(instance, args)

      val bytesCallResult = if (callResult != null) command.serviceResultSerializers(0).toBytes(callResult.asInstanceOf[BaseSerializable]) else Array.emptyByteArray

      bytesCallResult
    })
  }

  def dispatch(serviceName: String, args: Array[_ <: AnyRef], kwargs: mutable.Map[String, _ <: AnyRef]): Array[Array[Byte]] = {
    Array(query(serviceName)(args.map(_.asInstanceOf[ByteString])))
  }

  private def query(serviceName: String) = {
    try {
      // v2: auto register
      if (!handlers.contains(serviceName) && serviceName.startsWith("/v2/")) {
        register(serviceName, null)
      }
      handlers(serviceName)
    } catch {
      case e: Exception =>
        throw new CommandRoutingException(serviceName, e)
    }
  }


  def register_handler(serviceName: String, handler: (Array[ByteString]) => Array[Byte]): Unit = {
    if (handlers.contains(serviceName)) {
      throw new IllegalStateException(s"Service ${serviceName} has been registered")
    }
    handlers.put(serviceName, handler)
    logInfo(s"[COMMAND] registered ${serviceName}")
  }
}