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

package com.webank.eggroll.rollframe

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.util.concurrent.Executors

import com.webank.eggroll.core.meta.ErProcessor
import com.webank.eggroll.format.{FrameBatch, FrameDB, FrameReader, FrameWriter}

case class BatchData(headerSize:Int, header:Array[Byte], bodySize:Int, body:Array[Byte])
case class BatchID(id: Array[Byte])

trait TransferService

trait CollectiveTransfer

class NioCollectiveTransfer(nodes: Array[ErProcessor], timeout: Int = 600 * 1000) extends CollectiveTransfer {
  private lazy val clients = nodes.map { node =>
    (node.id, new NioTransferEndpoint().runClient(node.dataEndpoint.host, node.dataEndpoint.port))
  }.toMap
  def send(id: Long, path: String, frameBatch: FrameBatch):Unit = {
    clients(id).send(path, frameBatch)
  }
}

class NioTransferEndpoint {

  def runServer(host: String, port: Int): Unit = {

    val serverSocketChannel = ServerSocketChannel.open

    serverSocketChannel.socket.bind(new InetSocketAddress(host,port))
    val executors = Executors.newCachedThreadPool()

    while (true) {
      val socketChannel = serverSocketChannel.accept
      println(" new channel")
      executors.submit(new Runnable {
        override def run(): Unit = {
          val ch = socketChannel
          while (true) {
            val headLenBuf = ByteBuffer.allocateDirect(8)
            ch.read(headLenBuf)
            println("reading new batch")
            val headLen = headLenBuf.getLong(0)
            if(headLen > 1000 || headLen <=0 ) {
              println("head too long:"  + headLen + " port:" + port)
              throw new IllegalArgumentException("head too long:"  + headLen)
            }
            val headPathBuf = ByteBuffer.allocateDirect(headLen.toInt)
            ch.read(headPathBuf)
            headPathBuf.flip()
            val bytes = new Array[Byte](headLen.toInt)
            headPathBuf.get(bytes)
            val path = new String(bytes)
            headLenBuf.clear()
            headPathBuf.clear()
            val fr = new FrameReader(ch)
            FrameDB.queue(path, -1).writeAll(fr.getColumnarBatches())
            println("save finished:" + path)
          }
        }
      })
    }
  }
  var clientChannel: SocketChannel = _
  def runClient(host: String, port: Int): NioTransferEndpoint = {
    clientChannel = SocketChannel.open(new InetSocketAddress(host, port))
    println("NioCollectiveTransfer Connecting to Server on port ..." + port)
    this
  }

  def send(path: String, frameBatch: FrameBatch):Unit = {
    println("send start:" + path)
    val ch = clientChannel

    val pathBytes = path.getBytes()
    val headLen = pathBytes.length
    val headBuf = ByteBuffer.allocateDirect(8 + pathBytes.length)
    headBuf.putLong(headLen)
    headBuf.put(pathBytes)
    headBuf.flip()
    ch.write(headBuf)
    headBuf.clear()
    val fw = new FrameWriter(frameBatch,ch)
    fw.write()
    fw.close(false)


    println("send finished:" + path)
  }

}


