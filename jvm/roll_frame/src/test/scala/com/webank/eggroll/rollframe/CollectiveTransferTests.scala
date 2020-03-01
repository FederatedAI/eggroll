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

import com.webank.eggroll.format._
import org.junit.Test

class CollectiveTransferTests {
  private def getSchema(fieldCount:Int):String = {
    val sb = new StringBuilder
    sb.append("""{
                 "fields": [""")
    (0 until 1000).foreach{i =>
      if(i > 0) {
        sb.append(",")
      }
      sb.append(s"""{"name":"double$i", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}""")
    }
    sb.append("]}")
    sb.toString()
  }

  @Test
  def testSendNio():Unit = {
    val path = "aa"
    val fb = new FrameBatch(new FrameSchema(getSchema(100)), 100*20)
    val service = new NioTransferEndpoint
    val port = 8818
    val host = "127.0.0.1"
    val batchCount = 10
    new Thread() {
      override def run(): Unit = {
        try{
          service.runServer(host,port)
        } catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }.start()

    service.runClient(host, port)
    new Thread() {
      override def run(): Unit = {
        try{
          val start = System.currentTimeMillis()
          (0 until batchCount).foreach{ _ =>
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
    while (n<10){
      val fbs = FrameDB.queue(path, 1).readAll()
      fbs.hasNext
      println(fbs.next().rowCount)
      n+=1
    }
    println("recv time", System.currentTimeMillis() - start)

  }

  @Test
  def testSendNioMultiThreads():Unit = {
    val path = "aa"

    val service = new NioTransferEndpoint
    val host = "127.0.0.1"
    val port = 8818
    val batchCount = 10
    val fbs = (0 until batchCount).map(_=>new FrameBatch(new FrameSchema(getSchema(100)), 100*20)).toArray
    val clients = (0 until batchCount).map(_=>new NioTransferEndpoint).toArray

    new Thread() {
      override def run(): Unit = {
        try{
          service.runServer(host, port)
        } catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }.start()
    clients.foreach(_.runClient(host, port))
    (0 until batchCount).foreach{ i=>
    new Thread() {
      override def run(): Unit = {
        try{
          println()
          println("Thread: " + i)
          val start = System.currentTimeMillis()
            clients(i).send(path, fbs(i))

          println("send time", System.currentTimeMillis() - start)

        } catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }.start()}
    val start = System.currentTimeMillis()
    var n = 0
    while (n<10){
      println("n:" + n)
      val fbs = FrameDB.queue(path, 1).readAll()
      fbs.hasNext
      println(fbs.next().rowCount)
      n+=1
    }
    println("recv time", System.currentTimeMillis() - start)
  }

}
