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

package com.webank.eggroll.rollframe

import java.net.InetSocketAddress
import java.nio.channels.ServerSocketChannel

import com.webank.eggroll.format._
import org.junit.Test

class FrameTransferTests {

  @Test
  def testSendNio(): Unit = {
    val path = "aa"
    val fb = new FrameBatch(new FrameSchema(TestAssets.getSchema(1000)), 100 * 20)
    val service = new NioTransferEndpoint
    val port = 8817
    val host = "127.0.0.1"
    val batchCount = 10
    new Thread() {
      override def run(): Unit = {
        try {
          service.runServer(host, port)
        } catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }.start()

    Thread.sleep(1000)
    service.runClient(host, port)
    new Thread() {
      override def run(): Unit = {
        try {
          val start = System.currentTimeMillis()
          (0 until batchCount).foreach { _ =>
            service.send(path, fb)
          }
          println("send time", System.currentTimeMillis() - start)

        } catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }.start()
    val start = System.currentTimeMillis()
    var n = 0
    while (n < batchCount) {
      val fbs = FrameStore.queue(path, 1).readAll()
      fbs.hasNext
      println(fbs.next().rowCount)
      n += 1
    }
    println("recv time", System.currentTimeMillis() - start)
  }

  @Test
  def testSendNioMultiThreads(): Unit = {
    val path = "aa"
    val service = new NioTransferEndpoint
    val host = "127.0.0.1"
    val port = 8818
    val batchCount = 20
    val fbs = (0 until batchCount).map(_ => new FrameBatch(new FrameSchema(TestAssets.getSchema(1000)), 100 * 20)).toArray
    val clients = (0 until batchCount).map(_ => new NioTransferEndpoint).toArray

    new Thread() {
      override def run(): Unit = {
        try {
          service.runServer(host, port)
        } catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }.start()
    Thread.sleep(1000)
    val start = System.currentTimeMillis()
    clients.foreach(_.runClient(host, port))
    (0 until batchCount).foreach { i =>
      new Thread() {
        override def run(): Unit = {
          try {
            println()
            println("Thread: " + i)
            val start = System.currentTimeMillis()
            clients(i).send(path, fbs(i))

            println("send time", System.currentTimeMillis() - start)

          } catch {
            case e: Throwable => e.printStackTrace()
          }
        }
      }.start()
    }

    var n = 0
    while (n < batchCount) {
      println("n:" + n)
      val fbs = FrameStore.queue(path, 1).readAll()
      fbs.hasNext
      println(fbs.next().rowCount)
      n += 1
    }
    println("recv time", System.currentTimeMillis() - start)
  }

  @Test
  def testSendNioMultiThreadsOneClient(): Unit = {
    val path = "aa"
    val service = new NioTransferEndpoint
    val host = "127.0.0.1"
    val port = 8819
    val batchCount = 10
    val fbs = new FrameBatch(new FrameSchema(TestAssets.getSchema(1000)), 100 * 20)
    val client = new NioTransferEndpoint

    new Thread() {
      override def run(): Unit = {
        try {
          service.runServer(host, port)
        } catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }.start()
    Thread.sleep(1000)
    client.runClient(host, port)
    val start = System.currentTimeMillis()
    (0 until batchCount - 1).foreach { i =>
      new Thread() {
        override def run(): Unit = {
          try {

            println("Thread: " + i)
            val start = System.currentTimeMillis()
            client.synchronized {
              client.send(path, fbs)
            }

            println("send time", System.currentTimeMillis() - start)
          } catch {
            case e: Throwable => e.printStackTrace()
          }
        }
      }.start()
    }
    // the last thread send fy spend long time.
    new Thread() {
      override def run(): Unit = {
        try {
          Thread.sleep(10000)
          println("Thread: " + (batchCount - 1).toString)
          val start = System.currentTimeMillis()
          client.synchronized {
            client.send(path, fbs)
          }
          println("send time", System.currentTimeMillis() - start)
        } catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }.start()

    var n = 0

    val fbs1 = FrameStore.queue(path, batchCount).readAll()
    while (fbs1.hasNext) {
      println("n:" + n)
      val fb = fbs1.next()
      assert(fb.fieldCount == 1000)
      n += 1
    }
    println("recv time", System.currentTimeMillis() - start)
  }

  @Test
  def testClientReceive(): Unit = {
    val path = "aa"
    val fb = new FrameBatch(new FrameSchema(TestAssets.getSchema(1000)), 100 * 20)
    FrameStore.queue(path, 1).append(fb)
    FrameStore.cache(path).append(fb)
    val service = new NioTransferEndpoint
    val port = 8820
    val host = "127.0.0.1"
    new Thread() {
      override def run(): Unit = {
        try {
          service.runServer(host, port)
        } catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }.start()
    Thread.sleep(1000)
    val client = new NioTransferEndpoint
    client.runClient(host, port)

    val queueFb = client.receive(path, 1)
    println(s"queue fb: ${queueFb.next().fieldCount}")
    val cacheFb = client.pull(path)
    println(s"cache fb: ${cacheFb.next().fieldCount}")
  }

  @Test
  def testIsReachable(): Unit ={
    assert(HttpUtil.isReachable("127.0.0.1"))
  }

  @Test
  def testCheckAvailablePort(): Unit ={
    val host = "127.0.0.1"
    val port = 8821
    val serverSocketChannel = ServerSocketChannel.open
    serverSocketChannel.bind(new InetSocketAddress(host, port))
    assert(!HttpUtil.checkAvailablePort(host,port))
    serverSocketChannel.close()
    assert(HttpUtil.checkAvailablePort(host,port))
    assert(HttpUtil.checkAvailablePort(host,port))
  }

  @Test
  def testGetAvailablePort(): Unit ={
    val host = "127.0.0.1"
    val originPort = HttpUtil.ORIGIN_PORT
    val serverSocketChannel = ServerSocketChannel.open
    serverSocketChannel.bind(new InetSocketAddress(host, originPort))
    val availablePort = HttpUtil.getAvailablePort(host)
    serverSocketChannel.close()
    assert(originPort<=availablePort)
  }
}
