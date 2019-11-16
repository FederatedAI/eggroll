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

import com.google.protobuf.{ByteString, Message => PbMessage}
import com.webank.eggroll.core.datastructure.RpcMessage

trait BaseSerializable extends Serializable {
  //def toBytes(): Array[Byte]
}

trait BaseDeserializable extends Serializable {
  //def fromBytes(bytes: Array[Byte]): Any
}

trait ErSerializer {
  def toBytes(baseSerializable: BaseSerializable): Array[Byte]
}

trait ErDeserializer {
  def fromBytes(bytes: Array[Byte]): Any
}

trait PbMessageSerializer extends ErSerializer {
  def toProto[T >: PbMessage](): T
  override def toBytes(baseSerializable: BaseSerializable): Array[Byte] = ???
  def toByteString(): ByteString = toProto().toByteString
  def toBytes(): Array[Byte] = toProto().toByteArray
}

trait PbMessageDeserializer extends ErDeserializer {
  def fromProto[T >: RpcMessage](): T

  override def fromBytes(bytes: Array[Byte]): Any = ???
}
