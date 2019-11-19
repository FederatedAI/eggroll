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

import com.webank.eggroll.core.meta.ErEndpoint
import com.webank.eggroll.core.meta.NetworkingModelPbMessageSerdes._
import com.webank.eggroll.core.serdes.RpcMessageSerdesFactory
import org.junit.Test

class TestSerdes {
  @Test
  def testSerdesFactory(): Unit = {
    // ErEndpoint => PbMessage => Bytes
    val serializer = RpcMessageSerdesFactory.newSerializer(classOf[ErEndpoint])
    val deserializer = RpcMessageSerdesFactory.newDeserializer(classOf[ErEndpoint])

    val serializer2 = RpcMessageSerdesFactory.newSerializer(ErEndpoint.getClass)
    val serializer3 = RpcMessageSerdesFactory.newSerializer(classOf[ErEndpoint])
    val erEndpoint = ErEndpoint("localhost", 20001)

    println(erEndpoint.toProto())
    val endpointBytes = serializer3.toBytes(erEndpoint)

    println(endpointBytes)

    val result = deserializer.fromBytes(endpointBytes)

    println(result)
  }
}
