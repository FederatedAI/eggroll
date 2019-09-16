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

package com.webank.eggroll.core.serdes

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.reflect.ClassTag

trait FunctorSerDes extends BaseSerializable with BaseDeserializable {
  def serialize(func: Any): Array[Byte]

  def deserialize[T](bytes: Array[Byte]): T
}

case class DefaultScalaFunctorSerdes(func: Any = null) extends FunctorSerDes {
  override def toBytes(): Array[Byte] = serialize(func)

  override def serialize(func: Any): Array[Byte] = {
    val bo = new ByteArrayOutputStream()
    try {
      new ObjectOutputStream(bo).writeObject(func)
      bo.toByteArray
    } finally {
      bo.close()
    }
  }

  override def fromBytes[T: ClassTag](bytes: Array[Byte]): T = deserialize(bytes)

  override def deserialize[T](bytes: Array[Byte]): T = {
    val bo = new ObjectInputStream(new ByteArrayInputStream(bytes))
    try {
      bo.readObject().asInstanceOf[T]
    } finally {
      bo.close()
    }
  }
}