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

import java.nio.{ByteBuffer, ByteOrder, DoubleBuffer}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.format._
import com.webank.eggroll.rollframe.embpython.{LocalThreadPythonInterp, PyInterpreter}
import com.webank.eggroll.util.SchemaUtil
import io.netty.util.internal.PlatformDependent
import jep.DirectNDArray
import junit.framework.TestCase
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BitVectorHelper, Float8Vector, IntVector}
import org.junit.{Before, Test}

import scala.collection.immutable.Range.Inclusive
import scala.util.Random

class FrameFormatTests extends Logging {
  @Before
  def setup(): Unit = {
    StaticErConf.addProperty("hadoop.fs.defaultFS", "file:///")
  }

  @Test
  def testNullableFields(): Unit = {
    val fb1 = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(4)), 3000)
    val path = "/tmp/unittests/RollFrameTests/file/test1/nullable_test"
    val adapter = FrameStore.file(path)
    adapter.writeAll(Iterator(fb1.sliceByColumn(0, 3)))
    adapter.close()
    val adapter2 = FrameStore.file(path)
    val fb2 = adapter2.readOne()
    assert(fb2.rowCount == 3000)
    fb1.clear()
    fb2.clear()
  }

  /**
   * mul-thread to write data on FrameBatch by columns faster than one thread
   * */
  @Test
  def testParallelFrameBatchByCol(): Unit = {
    //    System.setProperty("arrow.enable_unsafe_memory_access", "true")
    val fieldCount = 200
    val rowCount = 50000

    val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), rowCount)
    var start = System.currentTimeMillis()
    for {x <- 0 until fieldCount
         y <- 0 until rowCount} {
      zeroValue.writeDouble(x, y, 1)
    }
    println(s"big time = ${System.currentTimeMillis() - start} ms")
    val sliceFieldCount = 20
    val count = fieldCount / sliceFieldCount

    val slices = (0 until count).map { i =>
      zeroValue.sliceByColumn(i * sliceFieldCount, (i + 1) * sliceFieldCount)
    }

    start = System.currentTimeMillis()
    val latch = new CountDownLatch(count)
    (0 until count).foreach { x =>
      new Thread() {
        override def run(): Unit = {
          for (f <- slices(x).rootVectors) {
            if (f.!=(null)) {
              0.until(rowCount).foreach { i =>
                f.writeDouble(i, 1)
              }
            }
          }
          latch.countDown()
        }
      }.start()
    }
    latch.await(10, TimeUnit.SECONDS)
    println(s"part time1 = ${System.currentTimeMillis() - start} ms")

    start = System.currentTimeMillis()
    val latch1 = new CountDownLatch(count)
    sliceByColumn(zeroValue, count).foreach { inclusive: Inclusive =>
      new Thread() {
        override def run(): Unit = {
          for (f <- zeroValue.sliceByColumn(inclusive.start, inclusive.end).rootVectors) {
            if (f.!=(null)) {
              0.until(rowCount).foreach { i =>
                f.writeDouble(i, 1)
              }
            }
          }
          latch1.countDown()
        }
      }.start()
    }
    latch1.await()
    println(s"part time2 = ${System.currentTimeMillis() - start} ms")
    zeroValue.clear()
  }

  /**
   * mul-thread to write data on FrameBatch by rows has no significant improvement
   * */
  @Test
  def testParallelFrameBatchByRow(): Unit = {
    // way 1: FrameStore with several FrameBatch
    val jvmPath = "/parallel/fbs_part"
    val jvmPath1 = "/parallel/fbs_big"
    val jvmAdapter = FrameStore.cache(jvmPath)
    val jvmAdapter1 = FrameStore.cache(jvmPath1)
    val randomObj = new Random() // block in threads
    val fbCount = 6
    val fieldCount = 200
    val rowCount = 10000
    val rowCountBig = fbCount * rowCount
    (0 until fbCount).foreach { i =>
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), rowCount)
      for {x <- 0 until fieldCount
           y <- 0 until rowCount} {
        fb.writeDouble(x, y, randomObj.nextDouble())
        //          fb.writeDouble(x, y, 1)
      }
      jvmAdapter.append(fb)
    }
    var start = System.currentTimeMillis()
    val latch = new CountDownLatch(fbCount)
    (0 until fbCount).foreach { i =>
      new Thread() {
        override def run(): Unit = {
          val randomObj1 = new Random()
          val fbj = jvmAdapter.readOne()
          (0 until fbj.fieldCount).foreach(f =>
            (0 until fbj.rowCount).foreach { r =>
              fbj.writeDouble(f, r, randomObj1.nextDouble())
            }
          )
          latch.countDown()
        }
      }.start()
    }
    latch.await(10, TimeUnit.SECONDS)
    println(s"Time = ${System.currentTimeMillis() - start} ms")
    // way 2: FrameStore with a FrameBatch
    jvmAdapter1.append({
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), rowCountBig)
      fb
    })
    start = System.currentTimeMillis()
    val fbj1 = jvmAdapter1.readOne()
    (0 until fbj1.fieldCount).foreach(f =>
      (0 until fbj1.rowCount).foreach { r =>
        fbj1.writeDouble(f, r, randomObj.nextDouble())
      }
    )
    println(s"Time = ${System.currentTimeMillis() - start} ms")
    // way 3: FrameStore with a FrameBatch, but sliced by row
    start = System.currentTimeMillis()
    val latch1 = new CountDownLatch(fbCount)
    val fbBig = jvmAdapter1.readOne()
    sliceByRow(fbCount, rowCountBig).foreach { inclusive: Inclusive =>
      new Thread() {
        override def run(): Unit = {
          val randomObj1 = new Random()
          val fbu = fbBig.sliceByRow(inclusive.start, inclusive.end)
          (0 until fbu.fieldCount).foreach(f =>
            (0 until fbu.rowCount).foreach { r =>
              fbu.writeDouble(f, r, randomObj1.nextDouble())
            }
          )
          latch1.countDown()
        }
      }.start()
    }
    latch1.await(10, TimeUnit.SECONDS)
    println(s"Time = ${System.currentTimeMillis() - start} ms")
  }

  private def sliceByRow(parts: Int, rows: Int): List[Inclusive] = {
    val partSize = (parts + rows - 1) / parts
    (0 until parts).map { sid =>
      new Inclusive(sid * partSize, Math.min((sid + 1) * partSize, rows), 1)
    }.toList
  }

  @Test
  def TestFileFrameStore(): Unit = {
    // create FrameBatch data
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(2)), 100)
    for (i <- 0 until fb.fieldCount) {
      for (j <- 0 until fb.rowCount) {
        fb.writeDouble(i, j, j)
      }
    }
    // write FrameBatch data to File
    val filePath = "/tmp/unittests/RollFrameTests/file/test1/framedb_test"
    val fileWriteAdapter = FrameStore.file(filePath)
    fileWriteAdapter.writeAll(Iterator(fb))
    fileWriteAdapter.close()

    // read FrameBatch data from File
    val fileReadAdapter = FrameStore.file(filePath)
    val fbFromFile = fileReadAdapter.readOne()

    assert(fbFromFile.readDouble(0, 3) == 3.0)
  }

  @Test
  def testJvmFrameStore(): Unit = {
    val cols = 200
    val rows = 10000
    (0 until 5).foreach { i =>
      val begin = System.currentTimeMillis()
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(cols)), rows)
      fb.initZero()
      println(s"create:${System.currentTimeMillis() - begin} ms")
      val begin1 = System.currentTimeMillis()
      fb.close()
      println(s"close:${System.currentTimeMillis() - begin1} ms")
    }
    val fb1 = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(cols)), rows)
    fb1.initZero()
    //     write FrameBatch data to Jvm
    val jvmPath = "/tmp/unittests/RollFrameTests/jvm/test1/framedb_test"
    val jvmAdapter = FrameStore.cache(jvmPath)
    jvmAdapter.writeAll(Iterator(fb1))
    // read FrameBatch data from Jvm
    val fbFromJvm = jvmAdapter.readOne()
    assert(fbFromJvm.rowCount == rows)
    assert(fbFromJvm.fieldCount == cols)
  }

  @Test
  def testHdfsFrameStore(): Unit = {
    // create FrameBatch data
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(2)), 100)
    for (i <- 0 until fb.fieldCount) {
      for (j <- 0 until fb.rowCount) {
        fb.writeDouble(i, j, j)
      }
    }
    // write FrameBatch data to HDFS
    val hdfsPath = "/tmp/unittests/RollFrameTests/hdfs/test1/framedb_test/0"
    val hdfsWriteAdapter = FrameStore.hdfs(hdfsPath)
    hdfsWriteAdapter.writeAll(Iterator(fb))
    hdfsWriteAdapter.close() // must be closed

    // read FrameBatch data from HDFS
    val hdfsReadAdapter = FrameStore.hdfs(hdfsPath)
    val fbFromHdfs = hdfsReadAdapter.readOne()

    assert(fbFromHdfs.readDouble(0, 20) == 20.0)
  }

  @Test
  def testNetworkFrameStore(): Unit = {
    // start transfer server
    val service = new NioTransferEndpoint
    val port = 8818
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

    // create FrameBatch data
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(2)), 100)
    for (i <- 0 until fb.fieldCount) {
      for (j <- 0 until fb.rowCount) {
        fb.writeDouble(i, j, j)
      }
    }
    // write FrameBatch data to Network
    val networkPath = "/tmp/unittests/RollFrameTests/network/test1/framedb_test/0"
    val networkWriteAdapter = FrameStore.network(networkPath, host, port.toString)
    networkWriteAdapter.append(fb)
    //    networkWriteAdapter.writeAll(Iterator(fb))
    Thread.sleep(1000) // wait for QueueFrameDB insert the frame

    // read FrameBatch data from network
    val networkReadAdapter = FrameStore.network(networkPath, host, port.toString)
    networkReadAdapter.readAll().foreach(fb =>
      assert(fb.readDouble(0, 30) == 30.0)
    )
  }

  @Test
  def testQueueFrameStore(): Unit = {
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(2)), 100)
    fb.initZero()
    val path = "abc"
    val adapter = FrameStore.queue(path, 2)
    adapter.append(fb)
    adapter.append(fb)
    val fbs = adapter.readAll()

    assert(fbs.next().fieldCount == 2)
    assert(fbs.next().rowCount == 100)
  }

  def printTime(call: => Unit): Unit = {
    val startTime = System.currentTimeMillis()
    call
    println()
    println(s"use time : ${System.currentTimeMillis() - startTime}")
  }

  @Test
  def testSpecialArray(): Unit = {
    System.setProperty("arrow.enable_unsafe_memory_access", "true")

    def getSchemaArray(fieldCount: Int, length: Array[Int], classNum: Int): String = {
      val sb = new StringBuilder
      sb.append(
        """{
                 "fields": [""")
      (0 until fieldCount).foreach { i =>
        if (i > 0) {
          sb.append(",")
        }
        val curLength = length(i) * classNum
        sb.append(
          s"""{"name":"longarray$i", "type": {"name" : "fixedsizelist","listSize" : $curLength},
             |"children":[{"name":"data", "type": {"name" : "int","bitWidth" : 32,"isSigned":true}}]}""")
      }
      sb.append(",")
      sb.append(s"""{"name":"node_id", "type": {"name" : "int","bitWidth" : 32,"isSigned":true}}""")
      sb.append("]}")
      sb.toString().stripMargin
    }

    val featureNumber = 999
    val length = Array(715, 708, 718, 723, 720, 713, 709, 714, 716, 718, 712, 709, 704, 710, 711, 705, 705, 717, 707, 721, 717, 722, 719, 716, 713, 719, 719, 723, 716, 702, 718, 703, 714, 716, 718, 721, 725, 724, 701, 714, 714, 711, 722, 720, 724, 710, 725, 717, 715, 723, 716, 715, 711, 709, 732, 726, 706, 724, 715, 722, 730, 719, 723, 712, 710, 716, 723, 708, 724, 725, 706, 711, 714, 710, 716, 710, 706, 734, 708, 704, 718, 725, 720, 712, 713, 729, 707, 713, 716, 722, 703, 707, 716, 720, 715, 719, 716, 708, 722, 715, 713, 715, 723, 719, 712, 715, 717, 715, 711, 719, 716, 707, 718, 712, 716, 717, 717, 719, 724, 707, 718, 718, 714, 710, 716, 725, 710, 719, 725, 712, 717, 703, 726, 715, 725, 716, 713, 710, 715, 705, 716, 713, 716, 715, 712, 714, 720, 711, 710, 712, 719, 705, 723, 710, 710, 712, 716, 713, 720, 711, 717, 725, 717, 719, 716, 723, 721, 716, 706, 710, 717, 708, 712, 720, 714, 712, 711, 717, 713, 710, 709, 723, 713, 711, 722, 714, 717, 714, 711, 718, 721, 714, 715, 710, 706, 713, 715, 717, 715, 714, 715, 707, 723, 711, 725, 713, 722, 707, 708, 710, 719, 714, 720, 716, 711, 733, 700, 709, 717, 716, 715, 712, 716, 714, 720, 715, 714, 711, 723, 714, 718, 715, 720, 724, 717, 720, 710, 719, 705, 705, 724, 729, 713, 706, 715, 714, 706, 724, 723, 719, 724, 714, 708, 713, 717, 713, 714, 717, 724, 721, 721, 712, 719, 725, 710, 713, 707, 710, 720, 716, 729, 711, 726, 716, 708, 719, 713, 716, 720, 711, 724, 711, 716, 717, 725, 713, 717, 720, 708, 720, 715, 714, 716, 713, 712, 713, 718, 721, 714, 709, 712, 712, 718, 705, 705, 711, 721, 712, 725, 701, 716, 715, 696, 699, 713, 701, 720, 713, 719, 702, 710, 724, 715, 702, 724, 718, 712, 707, 711, 720, 710, 717, 717, 713, 715, 716, 716, 720, 724, 716, 714, 719, 719, 717, 726, 716, 720, 712, 711, 714, 714, 720, 717, 718, 716, 715, 713, 716, 726, 718, 716, 712, 715, 719, 730, 709, 714, 720, 715, 722, 708, 713, 712, 706, 713, 713, 714, 710, 718, 716, 720, 717, 724, 713, 718, 718, 724, 726, 719, 722, 713, 722, 719, 716, 718, 717, 710, 709, 705, 723, 715, 710, 715, 720, 715, 721, 718, 724, 721, 715, 712, 717, 715, 716, 712, 707, 717, 717, 723, 721, 712, 714, 717, 717, 722, 720, 711, 713, 713, 710, 725, 709, 705, 723, 716, 714, 715, 719, 705, 722, 722, 714, 714, 712, 715, 724, 706, 702, 718, 718, 712, 722, 713, 716, 719, 710, 710, 717, 711, 717, 720, 715, 713, 715, 717, 705, 714, 710, 721, 714, 707, 704, 724, 729, 710, 711, 706, 717, 713, 725, 711, 723, 726, 716, 718, 721, 718, 710, 706, 724, 715, 711, 729, 715, 702, 728, 723, 723, 710, 711, 717, 723, 715, 712, 705, 719, 720, 709, 714, 708, 715, 715, 728, 709, 710, 715, 711, 718, 723, 716, 708, 710, 729, 717, 710, 719, 716, 713, 708, 726, 720, 700, 721, 714, 721, 718, 718, 716, 716, 712, 716, 723, 719, 712, 715, 714, 711, 704, 714, 714, 708, 724, 713, 714, 717, 701, 720, 715, 732, 708, 710, 728, 709, 718, 718, 719, 709, 720, 719, 718, 714, 709, 718, 710, 720, 709, 721, 715, 708, 707, 710, 720, 709, 712, 711, 718, 727, 714, 709, 720, 718, 712, 709, 714, 711, 709, 718, 708, 709, 711, 720, 718, 713, 719, 717, 708, 708, 718, 719, 716, 736, 712, 717, 715, 720, 716, 718, 710, 707, 715, 718, 713, 716, 723, 711, 711, 712, 727, 705, 708, 711, 727, 718, 711, 710, 701, 723, 713, 702, 726, 721, 722, 712, 710, 720, 717, 716, 718, 712, 715, 711, 720, 720, 709, 708, 715, 713, 724, 712, 706, 698, 724, 714, 710, 715, 722, 709, 717, 716, 710, 715, 715, 718, 706, 711, 720, 722, 707, 711, 720, 715, 709, 720, 714, 712, 707, 711, 716, 711, 705, 716, 718, 712, 710, 710, 713, 723, 706, 709, 711, 716, 706, 709, 714, 718, 714, 717, 714, 719, 713, 708, 719, 711, 712, 712, 710, 707, 713, 710, 712, 717, 717, 712, 714, 719, 711, 729, 719, 712, 711, 706, 712, 725, 715, 724, 709, 714, 715, 713, 711, 714, 724, 711, 708, 723, 714, 719, 715, 715, 721, 709, 702, 722, 735, 717, 720, 722, 714, 725, 721, 723, 714, 712, 723, 713, 713, 720, 707, 711, 712, 719, 723, 711, 722, 708, 712, 709, 723, 717, 712, 711, 708, 703, 717, 713, 709, 713, 735, 721, 728, 721, 726, 727, 707, 702, 718, 719, 725, 726, 710, 710, 719, 721, 723, 714, 705, 727, 715, 718, 715, 710, 716, 719, 711, 710, 713, 721, 714, 722, 715, 724, 708, 712, 709, 713, 713, 712, 712, 720, 719, 711, 715, 723, 725, 713, 714, 720, 707, 719, 713, 706, 730, 707, 718, 710, 703, 715, 720, 724, 710, 721, 716, 716, 707, 707, 703, 727, 718, 723, 721, 720, 723, 715, 713, 720, 703, 719, 705, 704, 712, 708, 721, 721, 708, 706, 720, 706, 718, 718, 721, 721, 715, 721, 709, 713, 716, 714, 703, 730, 716, 707, 707, 709, 722, 720, 709, 715, 712, 711, 712, 719, 716, 711, 717, 713, 705, 716, 719, 708, 711, 733, 718, 719, 724, 714, 712, 722, 725, 720, 717, 715, 710, 713, 720, 713, 721, 719, 708, 712, 706, 705, 704, 711, 719, 712, 709, 706, 716, 728, 703, 712, 713, 709, 712, 714, 712, 719, 708, 719, 715, 716, 717, 718, 718, 705, 716, 721, 718, 718, 711, 699, 715, 711, 713, 723, 711, 716, 717, 719, 712, 720, 705, 709, 716, 711, 724, 712, 714, 711, 708, 711, 716, 708, 708, 714, 709, 713, 713, 716, 710, 719, 716, 720, 708, 714, 709, 707, 709, 720)
    val classNum = 2
    val nodeNum = 10
    var start = System.currentTimeMillis()
    val schemaString = getSchemaArray(featureNumber, length, classNum)
    println(s"schemaString time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val x = new FrameBatch(new FrameSchema(schemaString), nodeNum)
    println(s"create time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    x.initZero()
    println(s"init time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    (0 until nodeNum).foreach(n => {
      (0 until featureNumber).foreach(i => {
        val arr = x.getArray(i, n)
        (0 until length(i) * classNum).foreach(j => arr.writeInt(j, 0))
      })
      x.writeInt(featureNumber, n, 1000)
    })
    //    FrameStore.cache("/tmp/unittests/RollFrameTests/file/test1/0").append(x)
    println(s"set value time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val fb1 = FrameUtils.fork(x)
    println(s"fork time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val by = FrameUtils.toBytes(x)
    println(s"toBytes time:${System.currentTimeMillis() - start} ms")
  }


  @Test
  def testUnSafe(): Unit = {
    System.setProperty("arrow.enable_unsafe_memory_access", "true")
    val cols = 1000
    val rows = 20000
    val loop = 2
    val random = new Random()
    (0 until loop).foreach { i =>
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(cols)), rows)
      var start = System.currentTimeMillis()
      (0 until cols).foreach(f =>
        (0 until rows).foreach { r =>
          fb.writeDouble(f, r, random.nextDouble())
        })
      println(s"write time = ${System.currentTimeMillis() - start} ms")
      start = System.currentTimeMillis()
      var sum = 0.0
      (0 until cols).foreach(f =>
        (0 until rows).foreach { r =>
          sum += fb.readDouble(f, r)
        })
      println(s"read time = ${System.currentTimeMillis() - start} ms")
    }
    println(s"property: ${System.getProperty("arrow.enable_unsafe_memory_access")}")
  }

  @Test
  def testLargeColumnFb(): Unit = {
    System.setProperty("arrow.enable_unsafe_memory_access", "true")
    val cols = 1000000
    val rows = 100
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(cols)), rows)
    var start = System.currentTimeMillis()
    (0 until cols).foreach(f =>
      (0 until rows).foreach { r =>
        fb.writeDouble(f, r, 1)
      })
    println(s"write time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    var sum = 0.0
    (0 until cols).foreach(f =>
      (0 until rows).foreach { r =>
        sum += fb.readDouble(f, r)
      })
    println(s"read time = ${System.currentTimeMillis() - start} ms")
  }

  @Test
  def testLargeRowFb(): Unit = {
    System.setProperty("arrow.enable_unsafe_memory_access", "true")
    val cols = 100
    val rows = 100000
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(cols)), rows)
    var start = System.currentTimeMillis()
    (0 until cols).foreach(f =>
      (0 until rows).foreach { r =>
        fb.writeDouble(f, r, 1)
      })
    println(s"write time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    var sum = 0.0
    (0 until cols).foreach(f =>
      (0 until rows).foreach { r =>
        sum += fb.readDouble(f, r)
      })
    println(s"read time = ${System.currentTimeMillis() - start} ms")
  }

  @Test
  def testOneFieldFb(): Unit = {
    System.setProperty("arrow.enable_unsafe_memory_access", "true")
    val cols = 1
    val rows = 122000
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(cols)), rows)
    var start = System.currentTimeMillis()
    (0 until cols).foreach(f =>
      (0 until rows).foreach { r =>
        fb.writeDouble(f, r, 1)
      })
    println(s"write time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    var sum = 0.0
    (0 until cols).foreach(f =>
      (0 until rows).foreach { r =>
        sum += fb.readDouble(f, r)
      })
    println(s"read time = ${System.currentTimeMillis() - start} ms")
  }

  @Test
  def testFloat8Vector(): Unit = {
    System.setProperty("arrow.enable_unsafe_memory_access", "true")
    val allocator = new RootAllocator(Long.MaxValue)
    val vector1 = new Float8Vector("double vector", allocator)
    val size = 100 * 1000
    vector1.allocateNew(size)
    vector1.setValueCount(size)
    for (i <- 0 until size) {
      vector1.set(i, i + 10)
    }

    val vector2 = new Float8Vector("double vector", allocator)
    vector2.allocateNew(size)
    vector2.setValueCount(size)
    for (i <- 0 until size) {
      vector2.set(i, i + 100)
    }
    //    val y = PlatformDependent.allocateMemory(10000)
    //    val df = PlatformDependent.allocateDirectNoCleaner(size * 2 * 8)
    val address = PlatformDependent.allocateMemory(size * 2 * 8)
    val df = PlatformDependent.directBuffer(address, size * 2 * 8)
    df.order(ByteOrder.LITTLE_ENDIAN)
    val start = System.currentTimeMillis()
    PlatformDependent.copyMemory(vector1.getDataBufferAddress, address, size * 8)
    PlatformDependent.copyMemory(vector2.getDataBufferAddress, address + size * 8, size * 8)
    println(s"time = ${System.currentTimeMillis() - start} ms")

    val interp: PyInterpreter = LocalThreadPythonInterp.interpreterThreadLocal.get()
    val dnd = new DirectNDArray[DoubleBuffer](df.asDoubleBuffer(), size * 2)
    interp.setValue("dnd", dnd)
    interp.exec("dnd[0] = 20")
    val res = interp.getValue("dnd").asInstanceOf[DirectNDArray[DoubleBuffer]]
    assert(dnd.getData.get(0) == res.getData.get(0))
    PlatformDependent.freeDirectNoCleaner(df)
  }

  @Test
  def testListVector(): Unit = {
    System.setProperty("arrow.enable_unsafe_memory_access", "true")
    val schema =
      """
      {
        "fields": [
          {"name":"doublelist0", "type": {"name" : "list"},
             "children":[{"name":"$data$", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}]
           }
        ]
      }
      """.stripMargin
    val rowCount = 4
    val perListLength = 10000
    val random = Random
    val batch = new FrameBatch(new FrameSchema(schema), rowCount)
    var start = System.currentTimeMillis()
    (0 until rowCount).foreach { i =>
      batch.getList(0, i, perListLength)
    }

    println(s"Time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    (0 until rowCount).foreach { i =>
      val row = batch.getList(0, i)
      (0 until perListLength).foreach(j => row.writeDouble(j * random.nextDouble().toInt, 0))
    }
    //    batch.initZero()
    println(s"Time = ${System.currentTimeMillis() - start} ms")
  }

  @Test
  def testArrayVector(): Unit = {
    System.setProperty("arrow.enable_unsafe_memory_access", "true")
    val schema =
      """
      {
        "fields": [
        {"name":"doublearray1", "type": {"name" : "fixedsizelist","listSize" : 20},
            "children":[{"name":"$data$", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}]
          }
        ]
      }
      """.stripMargin

    val batch = new FrameBatch(new FrameSchema(schema), 2)
    batch.initZero()

    val r1 = batch.getArray(0, 0)
    val r2 = batch.getArray(0, 1)

    (0 until 20).foreach(i => r1.writeDouble(i, i))
    (0 until 20).foreach(i => r2.writeDouble(i, i))
    batch.close()
  }

  @Test
  def testFrameDataType(): Unit = {
    System.setProperty("arrow.enable_unsafe_memory_access", "true")
    val schema =
      """
      {
        "fields": [
          {"name":"double1", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}},
          {"name":"long1", "type": {"name" : "int","bitWidth" : 64,"isSigned":true}},
          {"name":"longarray1", "type": {"name" : "fixedsizelist","listSize" : 10},
            "children":[{"name":"$data$", "type": {"name" : "int","bitWidth" : 64,"isSigned":true}}]
          },
          {"name":"longlist1", "type": {"name" : "list"},
             "children":[{"name":"$data$", "type": {"name" : "int","bitWidth" : 64,"isSigned":true}}]
           }
        ]
      }
      """.stripMargin

    val batch = new FrameBatch(new FrameSchema(schema), 2)
    batch.writeDouble(0, 0, 1.2)
    batch.writeLong(1, 0, 22)
    val arr = batch.getArray(2, 0)
    val list0 = batch.getList(3, 0, 3)
    val list2 = batch.getList(3, 1, 4)
    batch.getList(3, 2, 5)
    val list0Copy = batch.getList(3, 0)
    //    list.valueCount(3)
    list0.writeLong(2, 33)
    list0.writeLong(0, 44)
    list2.writeLong(3, 55)
    (0 until 10).foreach(i => arr.writeLong(i, i * 100 + 1))
    val outputStore = FrameStore.file("/tmp/unittests/RollFrameTests/file/test1/type_test")

    outputStore.append(batch)
    outputStore.close()
    assert(batch.readDouble(0, 0) == 1.2)
    assert(batch.readLong(1, 0) == 22)
    assert(arr.readLong(0) == 1)
    assert(list0.readLong(2) == 33)
    assert(list0.readLong(0) == 44)
    assert(list0Copy.readLong(0) == 44)

    val inputStore = FrameStore.file("/tmp/unittests/RollFrameTests/file/test1/type_test")
    for (b <- inputStore.readAll()) {
      assert(b.readDouble(0, 0) == 1.2)
      assert(b.readLong(1, 0) == 22)
      assert(b.getArray(2, 0).readLong(0) == 1)
      assert(b.getList(3, 0).readLong(0) == 44)
      assert(b.getList(3, 0).readLong(2) == 33)
      assert(b.getList(3, 1).readLong(3) == 55)
    }
  }

  @Test
  def testReadWrite(): Unit = {
    val schema =
      """
        {
          "fields": [
          {"name":"double1", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}},
          {"name":"double2", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}},
          {"name":"double3", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}
          ]
        }
      """.stripMargin
    val path = "/tmp/unittests/RollFrameTests/file/testColumnarWrite/0"
    val cw = new FrameWriter(new FrameSchema(schema), BlockDeviceAdapter.file(path))
    val valueCount = 10
    val fieldCount = 3
    val batchSize = 5
    cw.write(valueCount, batchSize,
      (fid, cv) => (0 until valueCount).foreach(
        n => cv.writeDouble(n, fid * valueCount + n * 0.5)
      )
    )
    cw.close()
    val cr = new FrameReader(path)
    for (cb <- cr.getColumnarBatches()) {
      for (fid <- 0 until fieldCount) {
        val cv = cb.rootVectors(fid)
        for (n <- 0 until cv.valueCount) {
          assert(cv.readDouble(n) == fid * valueCount + n * 0.5)
        }
      }
    }
    cr.close()
  }

  @Test
  def testMemoryCopy(): Unit = {
    // input frame
    val fieldCount = 1
    val rowCount = 5
    val schema = SchemaUtil.getDoubleSchema(fieldCount)
    val fb = new FrameBatch(new FrameSchema(schema), rowCount)
    for (i <- 0 until rowCount) {
      fb.writeDouble(0, i, 1 + i)
    }
    for (i <- 0 until rowCount) {
      println(fb.readDouble(0, i))
    }
    // memory to heap
    val fb1 = new FrameBatch(new FrameSchema(schema), rowCount)
    val to = fb1.rootVectors(0).fieldVector
    PlatformDependent.copyMemory(fb.rootVectors(0).fieldVector.getDataBuffer.memoryAddress, to.getDataBuffer.memoryAddress, to.getDataBuffer.capacity())
    for (i <- 0 until rowCount) {
      BitVectorHelper.setValidityBit(to.getValidityBuffer, i, 1)
    }
    for (i <- 0 until rowCount) {
      println(fb1.readDouble(0, i))
    }
    println
    // this method create vector which has new buffer

    // FrameBatch default little endian，the same as cpp
    val byteBuffer = fb.rootVectors(0).fieldVector.getDataBuffer.asNettyBuffer().nioBuffer()
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    byteBuffer.asDoubleBuffer().put(Array(1.0, 1.0, 1.0, 1.0, 1.0))
    for (i <- 0 until rowCount) {
      println(byteBuffer.getDouble())
    }
    println
    println(fb.readDouble(0, 3))

  }

  /**
   * There are three way to copy data from heap to direct memory.
   * 1. two for loop is most time-consuming
   * 2. using
   */
  @Test
  def testHeapToDirect(): Unit = {
    val fieldCount = 1
    val rowCount = 5000000
    val schema = SchemaUtil.getDoubleSchema(fieldCount)
    val fb = new FrameBatch(new FrameSchema(schema), rowCount)

    val value1 = Array.fill[Double](rowCount)(1)
    val value2 = Array.fill[Double](rowCount)(2)

    var start = System.currentTimeMillis()
    val dataByteBuffer = fb.rootVectors(0).fieldVector.getDataBuffer.nioBuffer()
    dataByteBuffer.order(ByteOrder.LITTLE_ENDIAN)
    dataByteBuffer.asDoubleBuffer().put(value1)


    val validityByteBuffer = fb.rootVectors(0).fieldVector.getValidityBuffer
    val validityBits = Array.fill[Byte](validityByteBuffer.capacity().toInt)(-1)
    validityByteBuffer.setBytes(0, validityBits)

    println(s"method 1 time = ${System.currentTimeMillis() - start}")
    println(fb.readDouble(0, 19))

    // method 2: two for loop
    start = System.currentTimeMillis()
    for (i <- 0 until rowCount) {
      fb.writeDouble(0, i, value2(i))
    }
    println(s"method 2 time = ${System.currentTimeMillis() - start}")
    println(fb.readDouble(0, 19))

    // method 3: toByte spent some time.
    // Attention: result error but ignore it because of not better way.
    //    val length = fb.rootVectors(0).fieldVector.getValueCapacity
    //    PlatformDependent.copyMemory(value3.map(_.toByte), 0,fb.rootVectors(0).fieldVector.getDataBuffer.memoryAddress, length)
    //    println(fb.readDouble(0,19))
  }

  @Test
  def testDirectToHeap(): Unit = {
    val rows = 5000000
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.oneDoubleFieldSchema), rows)
    fb.initZero()
    val start = System.currentTimeMillis()
    val res = FrameUtils.toDoubleArray(fb.rootVectors(0))
    println(s"time:${System.currentTimeMillis() - start} ms")
  }

  /**
   * better set -Xmx bigger
   */
  @Test
  def testFrameFork(): Unit = {
    val fieldCount = 100
    val rowCount = 10000 // total value count = rowCount * fbCount * fieldCount
    val schema = SchemaUtil.getDoubleSchema(fieldCount)
    var start = System.currentTimeMillis()
    println(s"create aa time = ${System.currentTimeMillis() - start} ms")

    start = System.currentTimeMillis()
    val rootSchema = new FrameSchema(schema)
    println(s"create shema time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val fb = new FrameBatch(rootSchema, rowCount)

    println(s"new FrameBatch time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    for {x <- 0 until fieldCount
         y <- 0 until rowCount} {
      fb.writeDouble(x, y, 1)
    }
    //    fb.initZero()
    println(s"set value time = ${System.currentTimeMillis() - start} ms\n")
    start = System.currentTimeMillis()
    val fb1 = FrameUtils.copy(fb)
    println(s"copy fb1 time= ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val fb2 = FrameUtils.copy(fb)
    println(s"copy fb2 time = ${System.currentTimeMillis() - start} ms\n")
    start = System.currentTimeMillis()
    val fb3 = FrameUtils.fork(fb)
    println(s"fork fb3 time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val fb4 = FrameUtils.fork(fb)
    println(s"fork fb4 time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    //    val fb5 = FrameUtils.transfer(fb)
    //    println(s"fork fb5 time = ${System.currentTimeMillis() - start} ms")


    val ads = fb.rootSchema.arrowSchema.getVector(0).getDataBufferAddress
    val ads1 = fb1.rootSchema.arrowSchema.getVector(0).getDataBufferAddress
    val ads2 = fb2.rootSchema.arrowSchema.getVector(0).getDataBufferAddress
    val ads3 = fb3.rootSchema.arrowSchema.getVector(0).getDataBufferAddress

    assert((ads != ads1) && (ads != ads2) && (ads != ads3))
    val delta = 0.00001
    assert(Math.abs(fb.readDouble(0, 0) - 1) < delta)
    assert(Math.abs(fb1.readDouble(2, 0) - 1) < delta)
    assert(Math.abs(fb2.readDouble(3, 0) - 1) < delta)
    assert(Math.abs(fb3.readDouble(4, 0) - 1) < delta)
    assert(Math.abs(fb4.readDouble(5, 0) - 1) < delta)
  }

  def sliceByColumn(frameBatch: FrameBatch, parallel: Int): List[Inclusive] = {
    val columns = frameBatch.rootVectors.length
    val quotient = columns / parallel
    val remainder = columns % parallel
    val processorsCounts = Array.fill(parallel)(quotient)
    (0 until remainder).foreach(i => processorsCounts(i) += 1)
    var start = 0
    var end = 0
    processorsCounts.map { count =>
      end = start + count
      val range = new Inclusive(start, end, 1)
      start = end
      range
    }.toList
  }
}
