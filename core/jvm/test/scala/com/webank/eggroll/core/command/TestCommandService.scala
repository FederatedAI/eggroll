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
import com.webank.eggroll.core.command.CommandModelPbMessageSerdes._
import com.webank.eggroll.core.command.CommandServiceGrpc.CommandServiceBlockingStub
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.di.Singletons
import com.webank.eggroll.core.factory.{GrpcChannelFactory, GrpcStubFactory}
import com.webank.eggroll.core.meta.ErEndpoint
import com.webank.eggroll.grpc.test.GrpcTest.HelloRequest
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.apache.commons.lang3.StringUtils
import org.junit.Test

class TestCommandService {

  @Test
  def testCommandRouting(): Unit = {
    val server = NettyServerBuilder.forPort(60000).addService(new CommandService()).build

    server.start()

    val sayHelloMethod = classOf[TestService].getMethod("sayHello", classOf[ByteString])
    val sayHelloServiceName = String.join(
      StringConstants.DOT,
      StringUtils.strip(sayHelloMethod.getDeclaringClass.getCanonicalName, StringConstants.DOLLAR),
      sayHelloMethod.getName)
    CommandRouter.register(sayHelloServiceName, sayHelloMethod.getParameterTypes)

    val sayHelloToPbMethod = classOf[TestService].getMethod("sayHelloToPbMessage", classOf[HelloRequest])
    val sayHelloToPbServiceName = String.join(
      StringConstants.DOT,
      StringUtils.strip(sayHelloToPbMethod.getDeclaringClass.getCanonicalName, StringConstants.DOLLAR),
      sayHelloToPbMethod.getName)
    CommandRouter.register(sayHelloToPbServiceName, sayHelloToPbMethod.getParameterTypes)

    val grpcChannelFactory = Singletons.get(classOf[GrpcChannelFactory])
    val grpcStubFactory = Singletons.get(classOf[GrpcStubFactory])

    val endpoint = ErEndpoint("localhost", 60000)
    val stub = grpcStubFactory.createGrpcStub(false, classOf[CommandServiceGrpc], endpoint, false).asInstanceOf[CommandServiceBlockingStub]
    /*    val sayHelloResponse = stub.call(CommandRequest(1L, sayHelloServiceName, Array("there".getBytes())).toProto())
        println(sayHelloResponse.getData.toStringUtf8)*/

    val sayHelloToGrpcResponse = stub.call(
      ErCommandRequest("2", sayHelloToPbServiceName, Array(HelloRequest.newBuilder().setMsg("grpc client").build().toByteArray)).toProto())
    println(sayHelloToGrpcResponse.getResults(0).toStringUtf8)
  }
}

class TestService {
  def sayHello(name: ByteString): String = {
    s"hello ${name.toStringUtf8} from server"
  }

  def sayHelloToPbMessage(name: HelloRequest): String = {
    s"hello pb message ${name.getMsg} from server"
  }
}


