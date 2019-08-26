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

package com.webank.eggroll.blockstore

import java.io._
import java.net.URI
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.google.protobuf.ByteString
import com.webank.eggroll.rollframe.{RollFrameGrpc, TransferServiceGrpc}
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver

import scala.collection.concurrent.TrieMap
import scala.collection.mutable


// stream access and optional random access
trait BlockStoreAdapter {
  def getInputStream():InputStream

  def getOutputStream():OutputStream
  // TODO: read/write by batch ? direct buffer?
  //  def write(f: OutputStream => Unit)
  def close():Unit = {}
}

object BlockStoreAdapter {
  def create(opts: Map[String, String]): BlockStoreAdapter = {
    opts.getOrElse("type", "file") match {
      case "jvm" => new JvmStoreAdapter(opts("path"), opts("size").toInt)
      case _ => new FileStoreAdapter(opts("path"))
    }
  }
}

class FileStoreAdapter(path: String) extends BlockStoreAdapter {
  var inputStream: InputStream = _
  var outputStream: OutputStream = _
  def getInputStream():InputStream = {
    inputStream = new FileInputStream(path)
    inputStream
  }

  override def getOutputStream(): OutputStream = {
    new File(path).getParentFile.mkdirs()
    outputStream = new FileOutputStream(path)
    outputStream
  }

  override def close(): Unit = {
    if(inputStream != null) {
      inputStream.close()
    }
    if(outputStream != null) {
      outputStream.close()
    }
  }
}

class JvmStoreAdapter(path: String, size: Int) extends BlockStoreAdapter {
  override def getInputStream(): InputStream = {
    // TODO: check null and size
    new ByteArrayInputStream(JvmStoreAdapter.get(path).get)
  }
  // grow size is expensive
  //  override def write(f: OutputStream => Unit): Unit = {
  //    val stream = new ExternalBytesOutputStream(size)
  //    try{
  //      f(stream)
  //      JvmStoreAdapter.put(path, stream.getBuffer)
  //    } finally {
  //      stream.close()
  //    }
  //  }
  override def getOutputStream(): OutputStream = new ExternalBytesOutputStream(path, size)
}

class ExternalBytesOutputStream(path: String, size: Int) extends ByteArrayOutputStream(size) {
  //  def getBuffer:Array[Byte] = buf

  override def close(): Unit = JvmStoreAdapter.put(path, buf)
}

object JvmStoreAdapter {
  private val data = new TrieMap[String, Array[Byte]]()

  def get(key: String): Option[Array[Byte]] = data.get(key)

  def put(key: String, bytes: Array[Byte]): Option[Array[Byte]] = data.put(key, bytes)
}

class GrpcStoreAdapter(opts: Map[String, String]) extends BlockStoreAdapter {
  val uri = new URI(opts("uri"))
  val client = TransferServiceGrpc.newStub(
    ManagedChannelBuilder.forAddress(uri.getHost,uri.getPort).usePlaintext().build())
  override def getInputStream(): InputStream = {
    val id = RollFrameGrpc.BatchID.newBuilder().setId(ByteString.copyFrom(uri.toString.getBytes()))
    opts.get("size").map( size => id.setSize(size.toInt))
    // TODO
    ???
    val stream = new GrpcInputStream(null)
    client.read(id.build(), stream)
    stream
  }

  override def getOutputStream(): OutputStream = {
    val id = RollFrameGrpc.BatchID.newBuilder().setId(ByteString.copyFrom(uri.toString.getBytes()))
    opts.get("size").map( size => id.setSize(size.toInt))
    new GrpcOutputStream(client.write(new StreamObserver[RollFrameGrpc.BatchID] {
      override def onNext(value: RollFrameGrpc.BatchID): Unit = {
        println("writed", value.toString)
      }

      override def onError(t: Throwable): Unit = ???

      override def onCompleted(): Unit = println("finished",this)
    }))
  }
}
class GrpcOutputStream(observer: StreamObserver[RollFrameGrpc.Batch],
                       batchSize: Int = 64 * 1024 * 1024) extends OutputStream {
  var batchWrote = 0
  var current:ByteString.Output = _
  val queue = new mutable.Queue[RollFrameGrpc.Batch]()
  override def write(b: Int): Unit = {
    if (current == null) {
      current = ByteString.newOutput(batchSize)
    }
    batchWrote += 4
    current.write(b)
    if(batchWrote >= batchSize) {
      writeBatch()
    }
  }

  private def writeBatch(): Unit ={
    observer.onNext(RollFrameGrpc.Batch.newBuilder().setData(current.toByteString).setId(
      RollFrameGrpc.BatchID.newBuilder().setSize(batchWrote)).build())
    current = null
    batchWrote = 0
  }

  override def close(): Unit = {
    // last one
    if(batchWrote > 0) {
      writeBatch()
    }
    observer.onCompleted()
  }
}
class GrpcInputStream(observer: StreamObserver[RollFrameGrpc.BatchID]) extends InputStream with StreamObserver[RollFrameGrpc.Batch] {
  var queue:BlockingQueue[RollFrameGrpc.Batch] = new LinkedBlockingQueue[RollFrameGrpc.Batch]()
  var current:InputStream = _
  var end = false
  var blockEnd = false
  var lastId:RollFrameGrpc.BatchID = _
  override def read(): Int = {
    if(end) {
      return -1
    }
    if(current.available() <= 0) {
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

class AdapterBridge(in: BlockStoreAdapter, out: BlockStoreAdapter) {
  // sample copy
  def transfer(): Unit ={
    val inStream = in.getInputStream()
    val outStream = out.getOutputStream()
    val buffer = Array.ofDim[Byte](1024)
    var count = -1
    while ({count = inStream.read(buffer); count > 0}){
      outStream.write(buffer, 0, count)
    }
  }
}