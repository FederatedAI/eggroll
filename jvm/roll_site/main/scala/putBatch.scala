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
package com.webank.eggroll.rollsite

import java.nio.ByteBuffer

import com.google.protobuf.ByteString
import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.datastructure.LinkedBlockingBroker
import com.webank.eggroll.rollpair.RollPairContext

object ScalaObjectPutBatch extends  App {
  def scalaPutBatch(name:String, namespace:String, key:ByteBuffer, value:ByteBuffer)= {
    val sid = "testing"
    val ctx = new RollPairContext(new ErSession(sid))
    val rp = ctx.load(namespace, name)

    var directBinPacketBuffer: ByteBuffer = ByteBuffer.allocateDirect(1<<10)

    //directBinPacketBuffer.order(ByteOrder.BIG_ENDIAN)
    //directBinPacketBuffer.put(NetworkConstants.TRANSFER_PROTOCOL_MAGIC_NUMBER) // magic num
    //directBinPacketBuffer.put(NetworkConstants.TRANSFER_PROTOCOL_VERSION) // protocol version
    //directBinPacketBuffer.putInt(4) // header length
    //directBinPacketBuffer.putInt(value.limit() + 4) // body size, (value len + header length)
    //directBinPacketBuffer.putInt(4) // key length (bytes)
    //directBinPacketBuffer.putInt(3)         // key
    //directBinPacketBuffer.putInt(value.limit()) // value length (bytes)
    directBinPacketBuffer.put(value)

    directBinPacketBuffer.flip()

    val broker = new LinkedBlockingBroker[ByteString]()
    broker.put(ByteString.copyFrom(directBinPacketBuffer))
    broker.signalWriteFinish()
    rp.putBatch(broker)
  }
}



