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

package com.webank.eggroll.blockdevice

import java.io._
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.google.protobuf.ByteString
import com.webank.eggroll.rollframe.RollFrameGrpc
import io.grpc.stub.StreamObserver

import scala.collection.concurrent.TrieMap
import scala.collection.mutable


// need random access?
trait BlockDeviceAdapter {
  def getInputStream(): InputStream

  def getOutputStream(): OutputStream

  def close(): Unit = {}
}

object BlockDeviceAdapter {
  def apply(opts: Map[String, String]): BlockDeviceAdapter = {
    opts.getOrElse("type", "file") match {
      case "cache" => new JvmBlockAdapter(opts("path"), opts("size").toInt)
      case _ => new FileBlockAdapter(opts("path"))
    }
  }

  def file(path: String): BlockDeviceAdapter = apply(Map("path" -> path, "type" -> "file"))
}

class FileBlockAdapter(path: String) extends BlockDeviceAdapter {
  var inputStream: InputStream = _
  var outputStream: OutputStream = _

  def getInputStream(): InputStream = {
    inputStream = new FileInputStream(path)
    inputStream
  }

  override def getOutputStream(): OutputStream = {
    new File(path).getParentFile.mkdirs()
    outputStream = new FileOutputStream(path)
    outputStream
  }

  override def close(): Unit = {
    if (inputStream != null) {
      inputStream.close()
    }
    if (outputStream != null) {
      outputStream.close()
    }
  }
}

class JvmBlockAdapter(path: String, size: Int) extends BlockDeviceAdapter {
  override def getInputStream(): InputStream = {
    // TODO: check null and size
    new ByteArrayInputStream(JvmBlockAdapter.get(path).get)
  }

  override def getOutputStream(): OutputStream = new ExternalBytesOutputStream(path, size)
}

class ExternalBytesOutputStream(path: String, size: Int) extends ByteArrayOutputStream(size) {

  override def close(): Unit = JvmBlockAdapter.put(path, buf)
}

object JvmBlockAdapter {
  private val data = new TrieMap[String, Array[Byte]]()

  def get(key: String): Option[Array[Byte]] = data.get(key)

  def put(key: String, bytes: Array[Byte]): Option[Array[Byte]] = data.put(key, bytes)
}

class GrpcOutputStream(observer: StreamObserver[RollFrameGrpc.Batch],
                       batchSize: Int = 64 * 1024 * 1024) extends OutputStream {
  var batchWrote = 0
  var current: ByteString.Output = _
  val queue = new mutable.Queue[RollFrameGrpc.Batch]()

  override def write(b: Int): Unit = {
    if (current == null) {
      current = ByteString.newOutput(batchSize)
    }
    batchWrote += 4
    current.write(b)
    if (batchWrote >= batchSize) {
      writeBatch()
    }
  }

  private def writeBatch(): Unit = {
    observer.onNext(RollFrameGrpc.Batch.newBuilder().setData(current.toByteString).setId(
      RollFrameGrpc.BatchID.newBuilder().setSize(batchWrote)).build())
    current = null
    batchWrote = 0
  }

  override def close(): Unit = {
    // last one
    if (batchWrote > 0) {
      writeBatch()
    }
    observer.onCompleted()
  }
}

class GrpcInputStream(observer: StreamObserver[RollFrameGrpc.BatchID])
  extends InputStream with StreamObserver[RollFrameGrpc.Batch] {
  var queue: BlockingQueue[RollFrameGrpc.Batch] = new LinkedBlockingQueue[RollFrameGrpc.Batch]()
  var current: InputStream = _
  var end = false
  var blockEnd = false
  var lastId: RollFrameGrpc.BatchID = _

  override def read(): Int = {
    if (end) {
      return -1
    }
    if (current.available() <= 0) {
      current = queue.take().getData.newInput()
    }
    current.read()
  }


  override def onNext(value: RollFrameGrpc.Batch): Unit = {
    queue.put(value)
    lastId = value.getId
  }

  override def onError(t: Throwable): Unit = {
    t.printStackTrace()
  }

  override def onCompleted(): Unit = {
    observer.onNext(lastId)
    observer.onCompleted()
    end = true
  }
}
