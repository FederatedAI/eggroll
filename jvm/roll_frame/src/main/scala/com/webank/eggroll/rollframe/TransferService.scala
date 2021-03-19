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

import java.net.{InetAddress, InetSocketAddress, SocketException}
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.util.concurrent.{ExecutorService, Executors, Future}

import scala.collection.mutable.ListBuffer
import com.webank.eggroll.core.meta.{ErProcessor, ErStore, ErStoreLocator}
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.format.{FrameBatch, FrameReader, FrameStore, FrameWriter, JvmFrameStore}

import scala.util.Random

// TODO: egg node use thread pool
trait FrameTransfer {
  def send(id: Long, path: String, frameBatch: FrameBatch): Unit = send(id, path, Iterator(frameBatch))

  def send(id: Long, path: String, frameBatch: Iterator[FrameBatch]): Unit

  def scatter(path: String, frameBatch: FrameBatch): Unit

  //  def broadcast(id: Long, path: String, frameBatch: FrameBatch): Unit = broadcast(path, Iterator(frameBatch))

  def broadcast(path: String, frameBatch: FrameBatch): Unit
}

// TODO: reduce duplicated code
class NioFrameTransfer(nodes: Array[ErProcessor], timeout: Int = 600 * 1000) extends FrameTransfer {
  val executors: ExecutorService = Executors.newCachedThreadPool()
  // will open all channel,but some don't be used
  private lazy val localHost: String = InetAddress.getLocalHost.getHostAddress
  private lazy val clients = nodes.map { node =>
    (node.id, new NioTransferEndpoint().runClient(node.transferEndpoint.host, node.transferEndpoint.port))
  }.toMap

  // the first node is roll
  // TODO: can specify roll
  private lazy val roll = clients(nodes(0).id)

  object Roll {
    def pull(path: String): Iterator[FrameBatch] = {
      roll.pull(path)
    }

    def broadcast(path: String, frameBatch: FrameBatch): Unit = {
      roll.broadcast(path, frameBatch)
    }

    private def checkStoreExists(path: String): Boolean = {
      roll.checkStoreExists(path)
    }

    def checkStoreExists(erStoreLocator: ErStoreLocator): Boolean = {
      // only check first partition
      val path = FrameStore.getStoreDir(erStoreLocator) + "0"
      checkStoreExists(path)
    }

    def getAvailablePort: Int = {
      roll.getAvailable
    }
  }

  def checkSameHost(host: String): Boolean = {
    localHost.equals(host) || host.equals("127.0.0.1")
  }

  override def send(id: Long, path: String, frameBatch: Iterator[FrameBatch]): Unit = {
    clients(id).send(path, frameBatch)
  }

  override def broadcast(path: String, frameBatch: FrameBatch): Unit = {
    val futures = new ListBuffer[Future[Unit]]
    clients.foreach { c =>
      futures.append(executors.submit(() => {
        c._2.broadcast(path, frameBatch)
      }))
    }
    try {
      futures.foreach { i =>
        i.get()
      }
    } catch {
      case e: Throwable => throw new RuntimeException("broadcast failed.", e)
    }
    //    clients.foreach(c => c._2.broadcast(path, frameBatch))
  }

  /**
   * The scatter op in here mya be different with another scatter op that split and transfer to every egg node.
   * Here only do the last job.
   *
   * @param path       queuePath
   * @param frameBatch localFrameBatch
   */
  override def scatter(path: String, frameBatch: FrameBatch): Unit = {
    // scatter are used in egg server
    val futures = new ListBuffer[Future[Unit]]
    clients.foreach { c =>
      futures.append(executors.submit(() => {
        // may be there can directly reuse broadcast instead of rebuild a scatter function like broadcast
        //        if (checkSameHost(c._2.getClientHost)) {
        //          // push to queue
        //          FrameStore.queue(path, -1).append(frameBatch)
        //        } else {
        //          c._2.scatter(path, frameBatch)
        //        }
        c._2.broadcast(path, frameBatch)
      }))
    }
    try {
      futures.foreach { i =>
        i.get()
      }
    } catch {
      case e: Throwable => throw new RuntimeException("scatter failed.", e)
    }
  }

  def addExcludeStore(store: ErStore): Unit = {
    val path = FrameStore.getStoreDir(store)
    addExcludeStore(path)
  }

  def addExcludeStore(namespace: String, name: String): Unit = {
    val path = FrameStore.getStoreDir(namespace, name)
    addExcludeStore(path)
  }

  def addExcludeStore(path: String): Unit = {
    clients.foreach {
      c =>
        c._2.addExclude(path)
    }
  }

  def deleteExcludeStore(store: ErStore): Unit = {
    val path = FrameStore.getStoreDir(store)
    deleteExcludeStore(path)
  }

  def deleteExcludeStore(namespace: String, name: String): Unit = {
    val path = FrameStore.getStoreDir(namespace, name)
    deleteExcludeStore(path)
  }

  def deleteExcludeStore(path: String): Unit = {
    clients.foreach {
      c =>
        c._2.deleteExclude(path)
    }
  }

  def resetExcludeStore(): Unit = {
    clients.foreach {
      c =>
        c._2.reSetExclude()
    }
  }

  def releaseStore(): Unit = {
    clients.foreach {
      c =>
        c._2.release()
    }
  }
}

case class NioTransferHead(action: String, path: String, batchSize: Int) {
  def write(ch: SocketChannel): Unit = {
    val pathBytes = path.getBytes
    val headLen = 4 + 4 + pathBytes.length
    val headBuf = ByteBuffer.allocateDirect(headLen + 4)
    headBuf.putInt(headLen)
    // TODO: use more convenient structure
    val actionInt = action match {
      case NioTransferHead.SEND => 1
      case NioTransferHead.BROADCAST => 2
      case NioTransferHead.RECEIVE => 3
      case NioTransferHead.PULL => 4
      case NioTransferHead.ADD_EXCLUDE => 5
      case NioTransferHead.DELETE_EXCLUDE => 6
      case NioTransferHead.RESET_EXCLUDE => 7
      case NioTransferHead.RELEASE => 8
      case NioTransferHead.CHECK_STORE => 9
      case NioTransferHead.SCATTER => 10
      case NioTransferHead.GET_AVAILABLE_PORT => 11
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
      case 1 => SEND
      case 2 => BROADCAST
      case 3 => RECEIVE
      case 4 => PULL
      case 5 => ADD_EXCLUDE
      case 6 => DELETE_EXCLUDE
      case 7 => RESET_EXCLUDE
      case 8 => RELEASE
      case 9 => CHECK_STORE
      case 10 => SCATTER
      case 11 => GET_AVAILABLE_PORT
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

  val SEND = "send"
  val BROADCAST = "broadcast"
  val RECEIVE = "receive"
  val PULL = "pull"
  val ADD_EXCLUDE = "add_exclude"
  val DELETE_EXCLUDE = "delete_exclude"
  val RESET_EXCLUDE = "reset_exclude"
  val RELEASE = "release"
  val CHECK_STORE = "check_store"
  val SCATTER = "scatter"
  val GET_AVAILABLE_PORT = "get_available_port"
}

class NioTransferEndpoint extends Logging {
  private var port = 0
  private var clientHost: String = _

  def getPort: Int = port

  def getClientHost: String = clientHost

  def runServer(host: String, port: Int): Unit = {
    logInfo("start Transfer server endpoint")
    val serverSocketChannel = ServerSocketChannel.open

    // TODO: deal with exception
    serverSocketChannel.bind(new InetSocketAddress(host, port))
    this.port = serverSocketChannel.socket.getLocalPort
    val executors = Executors.newCachedThreadPool()
    executors.submit(new Runnable {
      override def run(): Unit = {
        while (true) {
          val socketChannel = serverSocketChannel.accept // had client connected
          logInfo(s"receive a connected request,server ip: ${socketChannel.getLocalAddress}, remote ip: ${socketChannel.getRemoteAddress}")
          executors.submit(new Runnable {
            override def run(): Unit = {
              val ch = socketChannel
              while (true) {
                val head = NioTransferHead.read(ch)
                logInfo("server receive head = " + head)
                println("server receive head = " + head)
                // only support one batch now
                head.action match {
                  case NioTransferHead.SEND | NioTransferHead.SCATTER => // client send
                    val fr = new FrameReader(ch)
                    FrameStore.queue(head.path, head.batchSize).writeAll(fr.getColumnarBatches())
                  case NioTransferHead.BROADCAST => // client broadcast
                    try {
                      val fb = new FrameReader(ch).getColumnarBatches().next()
                      if (fb.isEmpty) {
                        throw new Exception("receive broadcast frame batch is empty")
                      }
                      FrameStore.cache(head.path).append(fb)
                    }
                    catch {
                      case e: Throwable =>
                        e.printStackTrace()
                        logError("receive broadcast failed:", e)
                    }
                  case NioTransferHead.RECEIVE => // client receive
                    head.write(ch)
                    writeFrameBatches(ch, FrameStore.queue(head.path, head.batchSize).readAll())
                  case NioTransferHead.PULL =>
                    head.write(ch)
                    writeFrameBatches(ch, FrameStore.cache(head.path).readAll())
                  case NioTransferHead.ADD_EXCLUDE =>
                    JvmFrameStore.addExclude(head.path)
                  case NioTransferHead.DELETE_EXCLUDE =>
                    JvmFrameStore.reSetExclude()
                  case NioTransferHead.RESET_EXCLUDE =>
                    JvmFrameStore.reSetExclude()
                  case NioTransferHead.RELEASE =>
                    JvmFrameStore.release()
                  case NioTransferHead.CHECK_STORE =>
                    val response = JvmFrameStore.checkFrameBatch(head.path)
                    val byteBuffer = ByteBuffer.allocateDirect(4)
                    byteBuffer.putInt(if (response) 1 else 0)
                    byteBuffer.flip()
                    ch.write(byteBuffer)
                    byteBuffer.clear()
                  case NioTransferHead.GET_AVAILABLE_PORT =>
                    val byteBuffer = ByteBuffer.allocateDirect(4)
                    byteBuffer.putInt(HttpUtil.getAvailablePort(host))
                    byteBuffer.flip()
                    ch.write(byteBuffer)
                    byteBuffer.clear()
                  case _ => throw new IllegalArgumentException(s"unsupported action:$head")
                }
                logInfo("save finished:" + head)
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
    clientHost = host
    //    logInfo("NioCollectiveTransfer Connecting to Server on port ..." + port)
    this
  }

  def writeFrameBatch(ch: SocketChannel, frameBatch: FrameBatch): Unit = {
    val fw = new FrameWriter(frameBatch, ch)
    fw.write()
    fw.close(false)
  }

  def writeFrameBatches(ch: SocketChannel, frameBatches: Iterator[FrameBatch]): Unit = {
    if (frameBatches.hasNext) {
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
    //    logInfo("send start:" + path)
    val ch = clientChannel
    NioTransferHead(action = NioTransferHead.SEND, path = path, batchSize = -1).write(ch)
    writeFrameBatches(ch, frameBatches)
    //    logInfo("send finished:" + path)
  }

  def scatter(path: String, frameBatch: FrameBatch): Unit = {
    val ch = clientChannel
    NioTransferHead(action = NioTransferHead.SCATTER, path = path, batchSize = -1).write(ch)
    writeFrameBatch(ch, frameBatch)
  }

  def broadcast(path: String, frameBatch: FrameBatch): Unit = {
    val ch = clientChannel
    NioTransferHead(action = NioTransferHead.BROADCAST, path = path, batchSize = -1).write(ch)
    writeFrameBatch(ch, frameBatch)
  }

  def receive(path: String, batchSize: Int = 1): Iterator[FrameBatch] = {
    val ch = clientChannel
    val head = NioTransferHead(action = NioTransferHead.RECEIVE, path = path, batchSize = batchSize)
    head.write(ch)
    val recvHead = NioTransferHead.read(ch)
    require(path == recvHead.path, s"error receive:$recvHead != $head")
    val fr = new FrameReader(ch)
    fr.getColumnarBatches()
  }

  def pull(path: String): Iterator[FrameBatch] = {
    val ch = clientChannel
    val head = NioTransferHead(action = NioTransferHead.PULL, path = path, batchSize = 1)
    head.write(ch)
    val recvHead = NioTransferHead.read(ch)
    require(path == recvHead.path, s"error pul:$recvHead != $head")
    val fr = new FrameReader(ch)
    fr.getColumnarBatches()
  }

  // TODO: can use non-block
  def addExclude(path: String): Unit = {
    NioTransferHead(action = NioTransferHead.ADD_EXCLUDE, path = path, batchSize = -1).write(clientChannel)
  }

  def deleteExclude(path: String): Unit = {
    NioTransferHead(action = NioTransferHead.DELETE_EXCLUDE, path = path, batchSize = -1).write(clientChannel)
  }

  def reSetExclude(): Unit = {
    NioTransferHead(action = NioTransferHead.RESET_EXCLUDE, path = "", batchSize = -1).write(clientChannel)
  }

  def release(): Unit = {
    NioTransferHead(action = NioTransferHead.RELEASE, path = "", batchSize = -1).write(clientChannel)
  }

  def checkStoreExists(path: String): Boolean = {
    NioTransferHead(action = NioTransferHead.CHECK_STORE, path = path, batchSize = -1).write(clientChannel)
    val response = ByteBuffer.allocateDirect(4)
    clientChannel.read(response)
    response.flip()
    if (response.getInt > 0) true else false
  }

  def getAvailable: Int = {
    NioTransferHead(action = NioTransferHead.GET_AVAILABLE_PORT, path = "", batchSize = -1).write(clientChannel)
    val response = ByteBuffer.allocate(4)
    clientChannel.read(response)
    response.flip()
    response.getInt
  }
}

object HttpUtil {
  val PORT_CHECK_TIME = 10
  val PORT_CHECK_STRIDE = 10
  val ORIGIN_PORT = 10001

  def isReachable(host: String): Boolean = {
    val a = InetAddress.getByName(host)
    a.isReachable(1000)
  }

  def checkAvailablePort(host: String, port: Int): Boolean = {
    var state = true
    try {
      val serverSocketChannel = ServerSocketChannel.open
      serverSocketChannel.bind(new InetSocketAddress(host, port))
      serverSocketChannel.close()
    } catch {
      case _: SocketException => state = false
    }
    state
  }

  def getAvailablePort(host: String): Int = {
    import scala.util.control.Breaks._
    var availablePort = this.ORIGIN_PORT
    val randomObj = new Random()
    breakable {
      for (_ <- 0 until this.PORT_CHECK_TIME) {
        if (HttpUtil.checkAvailablePort(host, availablePort)) {
          break
        }
        availablePort += randomObj.nextInt(this.PORT_CHECK_STRIDE)
      }
    }
    availablePort
  }
}


