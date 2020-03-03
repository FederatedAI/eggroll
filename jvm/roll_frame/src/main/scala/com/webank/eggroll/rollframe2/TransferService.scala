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

package com.webank.eggroll.rollframe2

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.util.concurrent.Executors

import com.webank.eggroll.core.meta.ErProcessor
import com.webank.eggroll.format.{FrameBatch, FrameReader, FrameStore, FrameWriter}

trait FrameTransfer {
  def send(id: Long, path: String, frameBatch: FrameBatch): Unit = send(id, path, Iterator(frameBatch))
  def send(id: Long, path: String, frameBatch: Iterator[FrameBatch]): Unit
  def scatter(path: String, frameBatches: Iterator[FrameBatch]): Unit
  def broadcast(path: String, frameBatches: Iterator[FrameBatch]): Unit
}

class NioFrameTransfer(nodes: Array[ErProcessor], timeout: Int = 600 * 1000) extends FrameTransfer {
  // will open all channel,but some don't be used
  private lazy val clients = nodes.map { node =>
    (node.id, new NioTransferEndpoint().runClient(node.transferEndpoint.host, node.transferEndpoint.port))
  }.toMap

  override def send(id: Long, path: String, frameBatch: Iterator[FrameBatch]): Unit = {
    clients(id).send(path, frameBatch)
  }

  override def broadcast(path: String, frameBatches: Iterator[FrameBatch]): Unit = {
    nodes.foreach { server =>
      frameBatches.foreach(fb => send(server.id, path, fb))
    }
  }

  override def scatter(path: String, frameBatches: Iterator[FrameBatch]): Unit = {

  }
}

case class NioTransferHead(action:String, path:String, batchSize:Int) {
  def write(ch: SocketChannel):Unit = {
    val pathBytes = path.getBytes()
    val headLen = 4 + 4 + pathBytes.length
    val headBuf = ByteBuffer.allocateDirect(headLen + 4)
    headBuf.putInt(headLen)
    val actionInt = action match {
      case "send" => 1
      case "receive" => 2
      case x => throw new IllegalArgumentException(s"unsupported action:$x")
    }
    headBuf.putInt(actionInt)
    headBuf.putInt(batchSize) // if send all batches, set batchSize = 1
    headBuf.put(pathBytes)
    headBuf.flip()
    ch.write(headBuf)
    headBuf.clear()
  }
}
object NioTransferHead {
  def read(ch: SocketChannel): NioTransferHead = {
    val headLenBuf = ByteBuffer.allocateDirect(4)
    ch.read(headLenBuf)
    headLenBuf.flip()
    val headLen = headLenBuf.getInt()
    val headBuf = ByteBuffer.allocateDirect(headLen.toInt)
    ch.read(headBuf)
    headBuf.flip()
    val action = headBuf.getInt() match {
      case 1 => "send"
      case 2 => "receive"
      case x => throw new IllegalArgumentException(s"unsupported action:$x")
    }

    val batchSize = headBuf.getInt()
    val bytes = new Array[Byte](headLen.toInt - 8)
    headBuf.get(bytes)
    val path = new String(bytes)
    headLenBuf.clear()
    headBuf.clear()
    NioTransferHead(action = action, path = path, batchSize = batchSize)
  }
}
class NioTransferEndpoint {
  private var port = 0
  def getPort:Int = port

  def runServer(host: String, port: Int): Unit = {
    println("start Transfer server endpoint")
    val serverSocketChannel = ServerSocketChannel.open

    serverSocketChannel.socket.bind(new InetSocketAddress(host, port))
    this.port = serverSocketChannel.socket.getLocalPort
    val executors = Executors.newCachedThreadPool()
    executors.submit(new Runnable {
      override def run(): Unit = {
        while (true) {
          val socketChannel = serverSocketChannel.accept // had client connected
          println(s"receive a connected request,server ip: ${socketChannel.getLocalAddress}, remote ip: ${socketChannel.getRemoteAddress}")
          executors.submit(new Runnable {
            override def run(): Unit = {
              println("currentThread = " + Thread.currentThread.getName)
              val ch = socketChannel
              while (true) {
                val head = NioTransferHead.read(ch)
                println("server receive head = " + head)
                // only support one batch now
                if(head.action == "send") { // client send
                  val fr = new FrameReader(ch)
                  FrameStore.queue(head.path, head.batchSize).writeAll(fr.getColumnarBatches())
                } else if (head.action  == "receive") { // client recv
                  head.write(ch)
                  writeFrameBatches(ch, FrameStore.queue(head.path, head.batchSize).readAll())
                } else {
                  throw new IllegalArgumentException(s"unsupported action:$head")
                }
                println("save finished:" + head)
              }
            }
          })
        }
      }
    })
  }

  var clientChannel: SocketChannel = _

  def runClient(host: String, port: Int): NioTransferEndpoint = {
    clientChannel = SocketChannel.open(new InetSocketAddress(host, port))
    println("NioCollectiveTransfer Connecting to Server on port ..." + port)
    this
  }

  def writeFrameBatches(ch:SocketChannel, frameBatches: Iterator[FrameBatch]):Unit = {
    if(frameBatches.hasNext){
      val fw = new FrameWriter(frameBatches.next(), ch)
      fw.write()
      while (frameBatches.hasNext) {
        fw.writeSibling(frameBatches.next())
      }
      fw.close(false)
    }
  }
  def send(path: String, frameBatch: FrameBatch): Unit = send(path, Iterator(frameBatch))
  def send(path: String, frameBatches: Iterator[FrameBatch]): Unit = {
    println("send start:" + path)
    val ch = clientChannel
    NioTransferHead(action = "send", path = path, batchSize = -1).write(ch)
    writeFrameBatches(ch, frameBatches)
    println("send finished:" + path)
  }
  def receive(path: String, batchSize:Int = 1): Iterator[FrameBatch] = {
    val ch = clientChannel
    val head = NioTransferHead(action = "receive", path = path, batchSize = batchSize)
    head.write(ch)
    val recvHead = NioTransferHead.read(ch)
    require(path == recvHead.path, s"error receive:$recvHead != $head")
    val fr = new FrameReader(ch)
    fr.getColumnarBatches()
  }
}


