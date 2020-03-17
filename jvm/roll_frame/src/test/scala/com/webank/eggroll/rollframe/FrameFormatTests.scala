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

import java.nio.{ByteBuffer, ByteOrder}

import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.format._
import com.webank.eggroll.util.SchemaUtil
import io.netty.util.internal.PlatformDependent
import junit.framework.TestCase
import org.apache.arrow.vector.BitVectorHelper
import org.junit.{Before, Test}

class FrameFormatTests {
  private val testAssets = TestAssets

  @Before
  def setup(): Unit = {
    StaticErConf.addProperty("hadoop.fs.defaultFS","file:///")
  }

  @Test
  def testNullableFields(): Unit = {
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(4)), 3000)
    val path = "/tmp/unittests/RollFrameTests/file/test1/nullable_test"
    val adapter = FrameStore.file(path)
    adapter.writeAll(Iterator(fb.sliceByColumn(0, 3)))
    adapter.close()
    val adapter2 = FrameStore.file(path)
    val fb2 = adapter2.readOne()
    assert(fb2.rowCount == 3000)
  }

  @Test
  def TestFileFrameDB(): Unit = {
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
  def testJvmFrameDB(): Unit = {
    // create FrameBatch data
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(2)), 100)
    for (i <- 0 until fb.fieldCount) {
      for (j <- 0 until fb.rowCount) {
        fb.writeDouble(i, j, j)
      }
    }
    // write FrameBatch data to Jvm
    val jvmPath = "/tmp/unittests/RollFrameTests/jvm/test1/framedb_test"
    val jvmAdapter = FrameStore.cache(jvmPath)
    jvmAdapter.writeAll(Iterator(fb))
    // read FrameBatch data from Jvm
    val fbFromJvm = jvmAdapter.readOne()

    assert(fbFromJvm.readDouble(0, 10) == 10.0)
  }

  @Test
  def testHdfsFrameDB(): Unit = {
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
  def testNetworkFrameDB(): Unit = {
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
  def testFrameDataType(): Unit = {
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
  def test(): Unit = {
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
    // 堆外内存拷贝到堆外内存例子
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

    // FrameBatch默认为小端存储数据，与C++一致
    val byteBuffer = fb.rootVectors(0).fieldVector.getDataBuffer.asNettyBuffer().nioBuffer()
    byteBuffer.order(ByteOrder.LITTLE_ENDIAN) // 改变读取顺序为小端
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
    val rowCount = 1000000
    val schema = SchemaUtil.getDoubleSchema(fieldCount)
    val fb = new FrameBatch(new FrameSchema(schema), rowCount)

    val value1 = Array.fill[Double](rowCount)(1)
    val value2 = Array.fill[Double](rowCount)(2)
    val value3 = Array.fill[Double](rowCount)(3)

    var start = System.currentTimeMillis()
    val dataByteBuffer = fb.rootVectors(0).fieldVector.getDataBuffer.nioBuffer()
    dataByteBuffer.order(ByteOrder.LITTLE_ENDIAN) // 改变读写顺序为小端,转为ByteBuffer后由小端变成大端，要人为修改回来。
    dataByteBuffer.asDoubleBuffer().put(value1)


    val validityByteBuffer = fb.rootVectors(0).fieldVector.getValidityBuffer
    val validityBits = Array.fill[Byte](validityByteBuffer.capacity().toInt)(-1)
    validityByteBuffer.setBytes(0, validityBits)

    println(s"way 2 time = ${System.currentTimeMillis() - start}")
    println(fb.readDouble(0, 19))

    // method 2: two for loop
    start = System.currentTimeMillis()
    for (i <- 0 until rowCount) {
      fb.writeDouble(0, i, value2(i))
    }
    println(System.currentTimeMillis() - start)
    println(fb.readDouble(0, 19))

    // method 3: toByte spent some time.
    // Attention: result error but ignore it because of not better way.
    //    val length = fb.rootVectors(0).fieldVector.getValueCapacity
    //    PlatformDependent.copyMemory(value3.map(_.toByte), 0,fb.rootVectors(0).fieldVector.getDataBuffer.memoryAddress, length)
    //    println(fb.readDouble(0,19))
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


    val ads = fb.rootSchema.arrowSchema.getVector(0).getDataBufferAddress
    val ads1 = fb1.rootSchema.arrowSchema.getVector(0).getDataBufferAddress
    val ads2 = fb2.rootSchema.arrowSchema.getVector(0).getDataBufferAddress
    val ads3 = fb3.rootSchema.arrowSchema.getVector(0).getDataBufferAddress
    val ads4 = fb4.rootSchema.arrowSchema.getVector(0).getDataBufferAddress

    assert((ads != ads1) && (ads != ads2) && (ads != ads3))
    val delta = 0.00001
    assert(Math.abs(fb.readDouble(0, 0) - 1) < delta)
    assert(Math.abs(fb1.readDouble(2, 0) - 1) < delta)
    assert(Math.abs(fb2.readDouble(3, 0) - 1) < delta)
    assert(Math.abs(fb3.readDouble(4, 0) - 1) < delta)
    assert(Math.abs(fb4.readDouble(5, 0) - 1) < delta)
  }

  @Test
  def testColumnVectors(): Unit = {
    val parts = 3
    val columns = 10
    val rows = 100
    val columnVectors = new ColumnVectors(columns)
    (0 until parts).foreach { i =>
      val frame = new ColumnVector(rows, columns)
      frame.writeDouble(0, 1)
      columnVectors.addElement(frame)
    }
    assert(parts == columnVectors.parts)
    assert(parts * rows * columns == columnVectors.rowCount)
  }

  @Test
  def testV1: Unit = {
  }

  @Test
  def testFrameBatchToColumnVectors(): Unit = {
    val fieldCount = 1000
    val rowCount = 10000 // total value count = rowCount * fbCount * fieldCount
    var start = System.currentTimeMillis()

    println(s"create shema time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), rowCount)

    println(s"new FrameBatch time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    for {x <- 0 until fieldCount
         y <- 0 until rowCount} {
      fb.writeDouble(x, y, 1)
    }

    println("value", fb.readDouble(3, 3))
    var end = System.currentTimeMillis()
    println(s"set time = ${end - start}")
    start = System.currentTimeMillis()

    start = System.currentTimeMillis()
    val cv = fb.toColumnVectors
    end = System.currentTimeMillis()
    println(end - start)
  }

  @Test
  def testColumnFrame(): Unit = {
    val matrixRows = 10000
    val matrixCols = 100
    var start = System.currentTimeMillis()
    val fb = new FrameBatch(new FrameSchema(FrameSchema.oneFieldSchema), matrixCols * matrixRows)
    println(s"create fb time = ${System.currentTimeMillis() - start} ms")
    start = System.currentTimeMillis()
    val sf = new ColumnFrame(fb, matrixCols)
    start = System.currentTimeMillis()
    (0 until sf.matrixCols).foreach(i => (0 until sf.matrixRows).foreach { j =>
      sf.write(i, j, i * j)
    })
    println(s"set cf time = ${System.currentTimeMillis() - start} ms")
    (0 until sf.matrixCols).foreach(i => (0 until sf.matrixRows).foreach { j =>
      TestCase.assertEquals(sf.read(i, j), i * j, TestAssets.DELTA)
    })

    println(sf.matrixCols)
    println(sf.matrixRows)
    println(sf.fb.rowCount)
    println(sf.read(2, 9))
    val adapter = FrameStore.cache("/tmp/unittests/RollFrameTests/file/test1/s1/0")
    adapter.append(sf.fb)
    start = System.currentTimeMillis()
    val fbC = adapter.readOne()
    val reCf = new ColumnFrame(fbC, matrixCols)
    println(reCf.fb.rowCount)
  }
}
