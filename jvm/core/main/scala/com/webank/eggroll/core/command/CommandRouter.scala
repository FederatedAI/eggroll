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
  private val serviceRouteTable = TrieMap[String, ErService]()
  private val messageParserMethodCache = mutable.Map[Class[_], Method]()
  private val defaultSerdesType = StaticErConf.getString(CoreConfKeys.CONFKEY_CORE_COMMAND_DEFAULT_SERDES_TYPE, SerdesTypes.PROTOBUF)

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
    if (serviceRouteTable.contains(serviceName)) {
      throw new IllegalStateException(s"Service ${serviceName} has been registered at: ${serviceRouteTable(serviceName)}")
    }

    val finalServiceName = if(serviceName.startsWith("/v2/")){
      val toks = serviceName.replaceFirst("/v2/","com/webank/").split("/")
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

    val routeToMethod = if(serviceParamTypes == null)
      finalRouteToClass.getMethods.find(_.getName == finalRouteToMethodName).get
     else MethodUtils.getAccessibleMethod(
      finalRouteToClass, finalRouteToMethodName, serviceParamTypes: _*)

    if (routeToMethod == null) {
      throw new NoSuchMethodException(s"accessible method not found for ${finalServiceName}")
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

    serviceRouteTable.put(serviceName, command)
    logInfo(s"[COMMAND] registered ${serviceName}")
  }

  def dispatch(serviceName: String, args: Array[_ <: AnyRef], kwargs: mutable.Map[String, _ <: AnyRef]): Array[Array[Byte]] = {
    val servicer = query(serviceName)
    if (servicer == null) {
      throw new IllegalStateException(s"service ${serviceName} has not been registered")
    }

    val method = servicer.routeToMethod
    val paramTypes = method.getParameterTypes
    var paramTypeName = "unknown"

    // todo:2: separate to SerDes
    // deserialization

    val realArgs = args.zip(servicer.serviceParamDeserializers).map {
      case (arg, deserializer) => {
        deserializer.fromBytes(arg.asInstanceOf[ByteString].toByteArray).asInstanceOf[Object]
      }
    }

    // actual call
    val callResult = method.invoke(servicer.callBasedInstance, realArgs: _*) // method.invoke(instance, args)

    val bytesCallResult = if (callResult != null) servicer.serviceResultSerializers(0).toBytes(callResult.asInstanceOf[BaseSerializable]) else Array.emptyByteArray

    val finalResult = Array[Array[Byte]](bytesCallResult)

    // serialization to response

    finalResult
  }

  def query(serviceName: String): ErService = {

    try {
      // v2: auto register
      if(!serviceRouteTable.contains(serviceName) && serviceName.startsWith("/v2/")) {
        register(serviceName, null)
      }
      serviceRouteTable(serviceName)
    } catch {
      case e: Exception =>
        throw new CommandRoutingException(serviceName, e)
    }
  }
}