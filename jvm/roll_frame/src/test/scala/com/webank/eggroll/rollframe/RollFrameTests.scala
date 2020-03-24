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
 */

package com.webank.eggroll.rollframe

import java.util.Random
import java.util.concurrent.{Callable, Executors}

import com.webank.eggroll.core.ErSession
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.{ErPartition, ErSessionMeta, ErStore, ErStoreLocator}
import com.webank.eggroll.format._
import com.webank.eggroll.rollframe.pytorch.{LibraryLoader, Matrices}
import com.webank.eggroll.util.SchemaUtil
import junit.framework.TestCase
import org.junit.{Before, Test}

import scala.collection.immutable.Range.Inclusive

/**
 * all unit test run on local mode
 */
class RollFrameTests {
  protected val ta = TestAssets
  protected val fieldCount = 100
  protected val rowCount = 100 // total value count = rowCount * fbCount * fieldCount
  // TODO: fbCount means a set of FrameBatch in one partition. fbCount value which larger then one maybe has bugs.
  protected val fbCount = 1 // the num of batch
  protected val supportTorch = false
  protected var ctx: RollFrameContext = _
  protected var inputStore: ErStore = _
  protected var inputHdfsStore: ErStore = _
  protected var inputTensorStore: ErStore = _
  protected val partitions_ = 3

  @Before
  def setup(): Unit = {
    ta.setMode("local")
    // TODO: 3in1 run fail
    ctx = ta.getRfContext(true)
//    val ctx1 = ctx.session.clusterManagerClient.getSession(ErSessionMeta(id = "debug-sid"))

    inputStore = ctx.createStore("test1", "a1", StringConstants.FILE, partitions_)
    inputHdfsStore = ctx.createStore("test1", "a1", StringConstants.HDFS, partitions_)
    inputTensorStore = ctx.createStore("test1", "t1", StringConstants.FILE, partitions_)

    // use torchScript or not
    if (supportTorch) {
      try {
        LibraryLoader.load
      } catch {
        case _: Throwable => println("error when loading jni")
      }
    }
  }

  private def writeCf(adapter: FrameStore): Unit = {
    val randomObj = new Random()
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.oneFieldSchemaString), fieldCount * rowCount)
    (0 until fieldCount * rowCount).foreach { i =>
      fb.writeDouble(0, i, randomObj.nextDouble())
    }

    println(s"row count: ${fb.rowCount}")
    adapter.append(fb)
    adapter.close()
  }

  // create Store for testing
  private def write(adapter: FrameStore): Unit = {
    val randomObj = new Random()
    (0 until fbCount).foreach { i =>
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), rowCount)
      for {x <- 0 until fieldCount
           y <- 0 until rowCount} {
        fb.writeDouble(x, y, randomObj.nextDouble())
        //          fb.writeDouble(x, y, 1)
      }
      println(s"FrameBatch order: $i,row count: ${fb.rowCount}")
      adapter.append(fb)
    }
    adapter.close()
  }

  private def writeSplice(adapter: FrameStore): Unit = {
    val sliceFieldCount = fieldCount / 2
    val randomObj = new Random()
    (0 until fbCount).foreach { i =>
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), rowCount)
      for {x <- 0 until fieldCount
           y <- 0 until rowCount} {
        fb.writeDouble(x, y, randomObj.nextDouble())
      }
      val sliceFb = fb.sliceByColumn(i * sliceFieldCount, (i + 1) * sliceFieldCount)
      println(s"FrameBatch order: $i,row count: ${sliceFb.fieldCount}")
      adapter.append(sliceFb)
    }
    adapter.close()
  }

  private def read(adapter: FrameStore): Unit = {
    var num = 0
    adapter.readAll().foreach { fb =>
      num += 1
      TestCase.assertEquals(fbCount, num)
      TestCase.assertEquals(fieldCount, fb.fieldCount)
      TestCase.assertEquals(rowCount, fb.rowCount)
    }
    adapter.close()
  }

  private def printContextMessage(): Unit = {
    val clusterManagerClient = ctx.session.clusterManagerClient
    val processors = ctx.session.processors
    println(s"ClusterManager host: ${clusterManagerClient.endpoint.host}, port:${clusterManagerClient.endpoint.port}")
    println(s"Processors counts:${processors.length}, list:")
    processors.indices.foreach(i => println(s"no $i: ${processors(i)}"))
  }

  @Test
  def testLoadRollFrame(): Unit = {
    // TODO: directly load rf failed
    val rf = ctx.load("test1", "a1")
    rf.mapBatch(fb => {
      println("to do assert", fb.readDouble(0, 0))
      fb
    })
  }

  /**
   * the other test methods depend on these frameBatches, and make sure of configuration of HDFS correctly.
   */
  @Test
  def testCreateDataStore(): Unit = {
    // TODO: more generally
    inputStore.partitions.indices.foreach { i =>
      //    create FrameBatch
      write(FrameStore(inputStore, i))
      //    create ColumnFrame
      writeCf(FrameStore(inputTensorStore, i))
    }
    write(FrameStore.hdfs("/tmp/unittests/RollFrameTests/hdfs/test2/a1/0"))
    write(FrameStore.hdfs("/tmp/unittests/RollFrameTests/hdfs/test2/a1/1"))
    write(FrameStore.hdfs("/tmp/unittests/RollFrameTests/hdfs/test2/a1/2"))
//    read(FrameStore.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/0"))
//    read(FrameStore.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/1"))
//    read(FrameStore.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/2"))
  }

  @Test
  def testStoreMetaInDb(): Unit = {
    // TODO: has bug, ErProcessor message didn't write into DB
    val input = ta.getRollFrameStore("a1", "test1")
    ctx.session.clusterManagerClient.deleteStore(input)
    val output = ctx.session.clusterManagerClient.getOrCreateStore(input)
    println(inputStore.partitions(0))
    println(output.partitions(0))
  }

  @Test
  def testNetworkToJvm(): Unit = {
    // 1.write to network
    val networkStore = ctx.forkStore(inputStore, "test1", "a1", StringConstants.NETWORK)
    networkStore.partitions.indices.foreach { i =>
      new Thread() {
        override def run(): Unit = {
          val fbs = FrameStore.file("/tmp/unittests/RollFrameTests/file/test1/a1/" + i).readAll()
          FrameStore(networkStore, i).writeAll(fbs)
        }
      }.start()
    }
    // TODO:2: pull data's not been implemented,  or should be tested in the same jvm process
    //    // 2.write to cache
    //    val cacheStore = ta.loadCache(networkStore)
    //    // 3.assert
    //    cacheStore.partitions.indices.foreach { i =>
    //      val fb = FrameDB(cacheStore, i).readOne()
    //      TestCase.assertEquals(fb.fieldCount, fieldCount)
    //      TestCase.assertEquals(fb.rowCount, rowCount)
    //    }
  }

  /**
   * a demo of slice a FrameBatch by rows and broadcast to the cluster to run aggregation
   * TODO: has bug
   */
  @Test
  def testSliceByRowAndAggregate(): Unit = {
    // 1. FrameBatch on root server
    val fb = FrameStore(inputStore, 0).readOne()

    // 2. distribute and load to caches
    val networkStore = ctx.forkStore(inputStore, "test1", "a1", StringConstants.NETWORK)
    val partitionsNum = networkStore.partitions.length
    val sliceRowCount = (partitionsNum + fb.rowCount - 1) / partitionsNum
    (0 until partitionsNum).foreach(i=>
      new Thread() {
        override def run(): Unit = {
          FrameStore(networkStore, i).append(fb.sliceRealByRow(i * sliceRowCount, sliceRowCount))
        }
      }.start())

   // 3. aggregate operation
    val rf = ctx.load(networkStore)
    val start = System.currentTimeMillis()
    val fieldCount = fb.fieldCount
    val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), 1)
    zeroValue.initZero()
    val outStore = ctx.createStore("test1", "a1Sr", StringConstants.FILE, totalPartitions = 1)

    rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          val fv = y.rootVectors(f)
          var sum = 0.0
          for (n <- 0 until fv.valueCount) {
            sum += 1
          }
          //          val fv = y.rootVectors(f)
          //          val sum = fv.valueCount
          x.writeDouble(f, 0, sum)
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      x
    }, { (a, b) =>
      for (i <- 0 until fieldCount) {
        a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    }, output = outStore)
    println(System.currentTimeMillis() - start)
    TestCase.assertEquals(FrameStore(outStore, 0).readOne().readDouble(0, 0), rowCount, TestAssets.DELTA)
  }

  @Test
  def testHdfsToJvm(): Unit = {
    val input = inputHdfsStore
    val output = ta.loadCache(input)
    TestCase.assertEquals(FrameStore(input, 0).readOne().readDouble(0, 0), FrameStore(output, 0).readOne().readDouble(0, 0), TestAssets.DELTA)
    TestCase.assertEquals(FrameStore(input, 1).readOne().readDouble(0, 0), FrameStore(output, 1).readOne().readDouble(0, 0), TestAssets.DELTA)
  }

  @Test
  def testMapBatchWithHdfs(): Unit = {
    val input =inputHdfsStore
    val output = ctx.forkStore(input, "test1", "a1map1", StringConstants.HDFS)
    val rf = ctx.load(input)
    rf.mapBatch({ cb =>
      val mapRf = new FrameBatch(cb.rootSchema)
      for (column <- 0 until mapRf.fieldCount) {
        for (row <- 0 until mapRf.rowCount) {
          mapRf.writeDouble(column, row, row)
        }
      }
      mapRf
    }, output = output)

    val mapFb0 = FrameStore(output, 0).readOne() // take first partition to assert
    TestCase.assertEquals(mapFb0.readDouble(0, 0), 0.0, TestAssets.DELTA)
    val mapFb1 = FrameStore(output, 1).readOne() // take second partition to assert
    TestCase.assertEquals(mapFb1.readDouble(2, 10), 10.0, TestAssets.DELTA)
  }

  @Test
  def testAggregateWithHdfs(): Unit = {
    var start = System.currentTimeMillis()
    val inStore = ta.loadCache(inputHdfsStore)
    val outStore = ctx.createStore("a1_aggregate", "test1", StringConstants.HDFS, totalPartitions = 1)
    val rf = ctx.load(inStore)
    println(System.currentTimeMillis() - start)
    start = System.currentTimeMillis()
    val fieldCountLocal = fieldCount
    val schema = SchemaUtil.getDoubleSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    zeroValue.initZero()

    rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          //          val sum = y.rowCount
          val fv = y.rootVectors(f)
          var sum = 0.0
          for (i <- 0 until fv.valueCount) {
            //            sum += fv.readDouble(i)
            sum += 1
          }
          x.writeDouble(f, 0, sum)
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      x
    }, { (a, b) =>
      for (i <- 0 until fieldCountLocal) {
        a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    }, threadsNum = 2, output = outStore)
    println(System.currentTimeMillis() - start)

    val resultFb = FrameStore(outStore, 0).readOne()
    TestCase.assertEquals(resultFb.readDouble(0, 0), inStore.partitions.length *rowCount , TestAssets.DELTA)
  }

  @Test
  def testMapBatch(): Unit = {
    val output = ctx.forkStore(inputStore, "test1", "a1map1", StringConstants.FILE)
    val rf = ctx.load(inputStore)
    val start = System.currentTimeMillis()
    rf.mapBatch({ cb =>
      val schema =
        """
        {
          "fields": [
          {"name":"double1", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}},
          {"name":"double2", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}
          ]
        }
      """.stripMargin
      val batch = new FrameBatch(new FrameSchema(schema), 2)
      batch.writeDouble(0, 0, 0.0 + cb.readDouble(0, 0))
      batch.writeDouble(0, 1, 0.1)
      batch.writeDouble(1, 0, 1.0)
      batch.writeDouble(1, 1, 1.1)
      batch
    }, output)
    println(s"map time = ${System.currentTimeMillis() - start}")
    ctx.load(output)
      .mapBatch { fb =>
        TestCase.assertEquals(fb.readDouble(0, 1), 0.1, TestAssets.DELTA)
        TestCase.assertEquals(fb.readDouble(1, 0), 1.0, TestAssets.DELTA)
        TestCase.assertEquals(fb.readDouble(1, 1), 1.1, TestAssets.DELTA)
        fb
      }
  }

  @Test
  def testReduceBatch(): Unit = {
    val input = inputStore
    val output = ctx.forkStore(inputStore, "test1", "a1_reduce", StringConstants.CACHE)
    val rf = ctx.load(input)
    rf.reduce({ (x, y) =>
      try {
        for (i <- 0 until x.fieldCount) {
          for (j <- 0 until x.rowCount) {
            x.writeDouble(i, j, x.readDouble(i, j) + y.readDouble(i, j))
          }
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      x
    }, output = output
    )
    val fb = FrameStore(input, 0).readOne()
    val fb1 = FrameStore(output, 0).readOne()
    TestCase.assertEquals(fb.fieldCount, fb1.fieldCount)
    TestCase.assertEquals(fb.rowCount, fb1.rowCount)
  }

  /**
   * A test function has more underlying code. User can pass it.
   */
  def testColumnarWrite1000(): Unit = {
    def pass(path: String): Unit = {
      val fieldCount = 10
      val schema = SchemaUtil.getDoubleSchema(fieldCount)
      val cw = new FrameWriter(new FrameSchema(schema), BlockDeviceAdapter.file(path))
      val valueCount = 500 * 100 / 2
      val batchSize = 500 * 100 / 2
      cw.write(valueCount, batchSize,
        (fid, cv) => (0 until batchSize).foreach(
          //        n => cv.writeDouble(n, fid * valueCount + n * 0.5)
          n => cv.writeDouble(n, fid * new Random().nextDouble())
        )
      )
      cw.close()
    }

    pass("/tmp/unittests/RollFrameTests/file/test1/b1/0")
    pass("/tmp/unittests/RollFrameTests/file/test1/b1/1")
  }

  @Test
  def testRollFrameAggregateBatch(): Unit = {
    val input = inputStore
    val output = ctx.createStore(namespace = "test1", name = "agg_1", totalPartitions = 1)
    val rf = ctx.load(input)
    val start = System.currentTimeMillis()
    val colsCount = fieldCount
    val schema = SchemaUtil.getDoubleSchema(colsCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until colsCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

    rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          for (i <- 0 until fv.valueCount) {
            //              sum += fv.readDouble(i) / 2
            sum += 1
          }
          x.writeDouble(f, 0, sum)
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      x
    }, { (a, b) =>
      for (i <- 0 until colsCount) {
        a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    }, output = output)
    println(System.currentTimeMillis() - start)
    val aggregateFb = FrameStore(output, 0).readOne()
    TestCase.assertEquals(aggregateFb.readDouble(0, 0), input.partitions.length * rowCount, TestAssets.DELTA)
  }

  /**
   * TODO: didn't finished, result is error
   */
  @Test
  def testRollFrameAggregateBy(): Unit = {
    var start = System.currentTimeMillis()
    val inStore = ta.getRollFrameStore("a1", "test1")
    val rf = ctx.load(inStore)
    println(System.currentTimeMillis() - start)
    start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = SchemaUtil.getDoubleSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 3)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

    rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          if (fv != null) {
            for (i <- 0 until fv.valueCount) {
              sum += 1
            }
            x.writeDouble(f, 0, sum)
            x.writeDouble(f, 1, sum)
            x.writeDouble(f, 2, sum)
            //          x.writeDouble(f,0, fv.valueCount)
          }

        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      //      x.columnarVectors.foreach(_.valueCount(5))
      print("x.valueCount", x.rootVectors(0).valueCount)
      x
    }, { (a, b) =>
      for (i <- 0 until fieldCount) {
        for (j <- 0 until 3) {
          if (b.rootVectors(i) != null) a.writeDouble(i, j, a.readDouble(i, j) + b.readDouble(i, j))
        }
      }
      a
    }, byColumn = true, output = ErStore(ErStoreLocator(storeType = "file", namespace = "test1", name = "r1byC")))
    println(System.currentTimeMillis() - start)
    //    val aggregateFb = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/r1byC/0").readOne()
    //    val aggregateFb1 = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/r1byC/1").readOne()
    //    println(aggregateFb.readDouble(0, 0))
    //    println(aggregateFb1.readDouble(5, 0))
  }

  @Test
  def testRollFrameMulti(): Unit = {
    val rf = ctx.load(inputStore)
    val start = System.currentTimeMillis()
    val colsCount = fieldCount
    val schema = SchemaUtil.getDoubleSchema(colsCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until colsCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

    rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          if (fv != null) {
            for (i <- 0 until fv.valueCount) {
              sum += fv.readDouble(i)
            }
            x.writeDouble(f, 0, sum)
            //          x.writeDouble(f,0, fv.valueCount)
          }
        }
      } catch {
        case t: Throwable => t.printStackTrace()
      }
      //      x.columnarVectors.foreach(_.valueCount(5))
      print("x.valueCount", x.rootVectors(0).valueCount)
      x
    }, { (a, b) =>
      for (i <- 0 until colsCount) {
        if (b.rootVectors(i) != null) a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    })

    val pool = Executors.newFixedThreadPool(2)
    val future1 = pool.submit(new Callable[Long] {
      override def call(): Long = {
        val start = System.currentTimeMillis()
        rf.aggregate(zeroValue, (x, _) => x, (a, _) => a)
        System.currentTimeMillis() - start
      }
    })

    val result1 = future1.get()

    val future2 = pool.submit(new Callable[Long] {
      override def call(): Long = {
        val start = System.currentTimeMillis()
        rf.aggregate(zeroValue, (x, _) => x, (a, _) => a)
        System.currentTimeMillis() - start
      }
    })
    val result2 = future2.get()
    println(System.currentTimeMillis() - start, result1, result2)
  }

  /**
   * a demo realizing matrix multiplication by using map function, but cost time
   */
  @Test
  @deprecated
  def testMatMulV1ByMapBatch(): Unit = {
    val input = ta.loadCache(inputStore)
    val output = ctx.forkStore(input, "a1Matrix", "test1", StringConstants.CACHE)
    val rf = ctx.load(input)
    val start = System.currentTimeMillis()
    val matrixCols = 1
    rf.mapBatch({ cb =>
      val matrix = Array.fill[Double](cb.fieldCount * matrixCols)(1.0)
      Matrices.matMulToFbV1(cb.toColumnVectors, matrix, cb.fieldCount, matrixCols)
    }, output)
    println(s"matMul time= ${System.currentTimeMillis() - start}")
    ctx.load(output).mapBatch { fb =>
      TestCase.assertEquals(fb.fieldCount, matrixCols)
      fb
    }
  }

  /**
   * a demo realizing matrix multiplication using ColumnVectors, but cost time
   */
  @Test
  @deprecated
  def testMatMulV1(): Unit = {
    val input = ta.loadCache(inputStore)
    val output = ctx.forkStore(input, "test1", "a1Matrix1", StringConstants.CACHE)
    val rf = ctx.load(input)
    val matrixRows = fieldCount
    val matrixCols = 1
    val matrix = Array.fill[Double](matrixRows * matrixCols)(1.0)
    val start = System.currentTimeMillis()
    rf.matMulV1(matrix, matrixRows, matrixCols, output)
    println(s"matMul time= ${System.currentTimeMillis() - start}")
    ctx.load(output)
      .mapBatch { fb =>
        TestCase.assertEquals(fb.fieldCount, matrixCols)
        fb
      }
  }

  @Test
  def testTorchScriptMap(): Unit = {
    val input = ta.loadCache(inputTensorStore)
    val output = ctx.forkStore(input, "test1", "a1TorchMap", StringConstants.FILE)
    val rf = ctx.load(input)
    val newMatrixCols = 2
    val parameters: Array[Double] = Array(fieldCount.toDouble) ++ Array(newMatrixCols.toDouble) ++ Array.fill[Double](fieldCount * newMatrixCols)(0.5);

    rf.torchMap("jvm/roll_frame/src/test/resources/torch_model_map.pt", parameters, output)
    TestCase.assertEquals(FrameStore(output, 0).readOne().rowCount, rowCount * newMatrixCols)
  }

  @Test
  def testTorchScriptMerge(): Unit = {
    val input = ta.loadCache(ctx.forkStore(inputTensorStore, "test1", "a1TorchMap", StringConstants.FILE))
    val output = ctx.createStore("test1", "a1TorchMerge", StringConstants.FILE, totalPartitions = 1)
    val rf = ctx.load(input)
    rf.torchMerge(path = "jvm/roll_frame/src/test/resources/torch_model_merge.pt", output = output)
    val result = FrameStore(output, 0).readOne()
    println(s"sum = ${result.readDouble(0, 0)},size = ${result.rowCount}")
  }

  @Test
  def testMatMul(): Unit = {
    val input = ta.loadCache(ta.getRollFrameStore("c1", "test1", StringConstants.FILE))
    val output = ta.getRollFrameStore("c1Matrix1", "test1", StringConstants.CACHE)
    val rf = ctx.load(input)
    val matrixRows = fieldCount
    val matrixCols = 1
    val matrix = Array.fill[Double](matrixRows * matrixCols)(1.0)
    val start = System.currentTimeMillis()
    rf.matMul(matrix, matrixRows, matrixCols, output)
    println(s"matMul time= ${System.currentTimeMillis() - start}")

    output.partitions.indices.foreach { i =>
      val resCF = new ColumnFrame(FrameStore(output, i).readOne(), matrixCols)
      TestCase.assertEquals(resCF.fb.rowCount, rowCount * matrixCols)
      println(s"partition $i, value = ${resCF.read(0, 0)}")
    }
  }
}

class RollFrameClusterTests extends RollFrameTests {
  @Before
  override def setup(): Unit = {
    ctx = ta.getRfContext(false)
    ta.setMode("local")
    try {
      LibraryLoader.load
    } catch {
      case _: Throwable => println("error when loading jni")
    }
  }

  @Test
  override def testMapBatch(): Unit = {
    super.testMapBatch()
  }
}
