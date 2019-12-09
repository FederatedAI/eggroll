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

import com.webank.eggroll.core.command.CommandModelPbMessageSerdes._
import org.junit.Test

import scala.collection.mutable

class TestCommandModel {

  @Test
  def testPbSerialize(): Unit = {
    val args = new Array[Array[Byte]](2)
    val hello = "hello"
    val world = "world"
    args(0) = hello.getBytes()
    args(1) = world.getBytes()

    val kwargs = mutable.Map[String, Array[Byte]]()
    kwargs += (hello -> hello.getBytes())
    kwargs += (world -> world.getBytes())
    val commandRequest = ErCommandRequest("1", "http://www.test.com", args, kwargs.toMap)

    val serialized = commandRequest.toProto

    println("serialized: " + serialized)
    val deserialized = serialized.fromProto

    println("deserialized: " + deserialized)
  }
}

