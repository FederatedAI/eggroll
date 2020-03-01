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

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.{ErPartition, ErProcessor, ErStore, ErStoreLocator}
import com.webank.eggroll.core.session.StaticErConf
import com.webank.eggroll.format._
import com.webank.eggroll.rollframe.RfServerLauncher.ctx
import com.webank.eggroll.rollframe.pytorch.linalg.Matrices
import com.webank.eggroll.rollframe.pytorch.native.LibraryLoader
import com.webank.eggroll.util.SchemaUtil
import junit.framework.TestCase
import org.junit.{Before, Test}

/**
 * all unit test run on local mode
 */
class RollFrameTests {
  protected val ta = TestAssets
  protected val fieldCount = 1000
  protected val rowCount = 1000 // total value count = rowCount * fbCount * fieldCount
  // TODO: fbCount means a set of FrameBatch in one partition. fbCount value which larger then one maybe has bugs.
  protected val fbCount = 1 // the num of batch

  protected var ctx:RollFrameContext = _
  @Before
  def setup(): Unit = {
    ta.setMode("local")
    ctx = ta.getRfContext(true)
    HdfsBlockAdapter.fastSetLocal()
    try{
      LibraryLoader.load
    } catch {
      case _: Throwable => println("error when loading jni")
    }
  }

  private def writeCf(adapter: FrameDB): Unit = {
    val randomObj = new Random()
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.oneFieldSchemaString), fieldCount * rowCount)
    (0 until fieldCount * rowCount).foreach { i =>
      fb.writeDouble(0, i, randomObj.nextDouble())
    }

    println(s"row count: ${fb.rowCount}")
    adapter.append(fb)
    adapter.close()
  }

  private def write(adapter: FrameDB): Unit = {
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

  private def writeSplice(adapter: FrameDB): Unit = {
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

  private def read(adapter: FrameDB): Unit = {
    var num = 0
    adapter.readAll().foreach { fb =>
      num += 1
      TestCase.assertEquals(fbCount, num)
      TestCase.assertEquals(fieldCount, fb.fieldCount)
      TestCase.assertEquals(rowCount, fb.rowCount)
    }
    adapter.close()
  }

  /**
   * the other test methods depend on these frameBatches, and make sure of configuration of HDFS correctly.
   */
  @Test
  def testCreateDataStore(): Unit = {
    val output = ta.getRollFrameStore("a1", "test1", StringConstants.FILE)
    val output1 = ta.getRollFrameStore("c1","test1",StringConstants.FILE)
    output.partitions.indices.foreach { i =>
      // create FrameBatch
      write(FrameDB(output, i))
      // create ColumnFrame
      writeCf(FrameDB(output1, i))
    }
        write(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/0"))
        write(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/1"))
        write(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/2"))
//        read(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/0"))
//        read(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/1"))
  }

  @Test
  def testNetworkToJvm(): Unit = {
    // 1.write to network
    val networkStore = ta.getRollFrameStore("a1", "test1", StringConstants.NETWORK)
    networkStore.partitions.indices.foreach { i =>
      new Thread() {
        override def run(): Unit = {
          val fbs = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/a1/" + i).readAll()
          FrameDB(networkStore, i).writeAll(fbs)
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
   */
  @Test
  def testSliceByRowAndAggregate(): Unit = {
    // 1. FrameBatch on root server
    val storeLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.FILE)
    val input = ErStore(storeLocator = storeLocator,
      partitions = Array(ErPartition(id = 0, storeLocator = storeLocator, processor = ta.localNode0)))
    val fb = FrameDB(input, 0).readOne()

    // 2. distribute and load to caches
    val networkStore = ta.getRollFrameStore("a1", "test1", StringConstants.NETWORK)
    val partitionsNum = networkStore.partitions.length
    val sliceRowCount = (partitionsNum + fb.rowCount - 1) / partitionsNum
    (0 until partitionsNum).foreach(i => FrameDB(networkStore, i).append(fb.sliceRealByRow(i * sliceRowCount, sliceRowCount)))
    val cacheStore = ta.loadCache(networkStore)
    (0 until partitionsNum).foreach(i => println(s"check id.$i partition's row number: ${FrameDB(cacheStore, i).readOne().rowCount}"))

    // 3. aggregate operation
    val rf = ctx.load(cacheStore)
    val start = System.currentTimeMillis()
    val fieldCount = fb.fieldCount
    val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), 1)
    zeroValue.initZero()
    val outStoreLocator = ErStoreLocator(name = "a1Sr", namespace = "test1", storeType = StringConstants.FILE)
    // TODO: aggregate output Store is inconsistent here
    val outStore = ta.getRollFrameStore("a1Sr", "test1", StringConstants.FILE)
    val outStore1 = ErStore(storeLocator = outStoreLocator, partitions = Array(
      ErPartition(id = 0, storeLocator = outStoreLocator, processor = ta.localNode0)))
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
    TestCase.assertEquals(FrameDB(outStore1, 0).readOne().readDouble(0, 0), rowCount, TestAssets.DELTA)
  }

  @Test
  def testHdfsToJvm(): Unit = {
    val input = ta.getRollFrameStore("a1", "test1", StringConstants.HDFS)
    val output = ta.loadCache(input)
    TestCase.assertEquals(FrameDB(input, 0).readOne().readDouble(0, 0), FrameDB(output, 0).readOne().readDouble(0, 0), TestAssets.DELTA)
    TestCase.assertEquals(FrameDB(input, 1).readOne().readDouble(0, 0), FrameDB(output, 1).readOne().readDouble(0, 0), TestAssets.DELTA)
  }

  @Test
  def testMapBatchWithHdfs(): Unit = {
    val input = TestAssets.getRollFrameStore("a1", "test1", StringConstants.HDFS)
    val output = TestAssets.getRollFrameStore("a1map", "test1", StringConstants.HDFS)

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

    val mapFb0 = FrameDB(output, 0).readOne() // take first partition to assert
    TestCase.assertEquals(mapFb0.readDouble(0, 0), 0.0, TestAssets.DELTA)
    val mapFb1 = FrameDB(output, 1).readOne() // take second partition to assert
    TestCase.assertEquals(mapFb1.readDouble(2, 10), 10.0, TestAssets.DELTA)
  }

  @Test
  def testAggregateWithHdfs(): Unit = {
    var start = System.currentTimeMillis()
    val inStore = ta.getRollFrameStore("a1", "test1", StringConstants.HDFS)
    // TODO: aggregate output Store is inconsistent here
    val outStore = ta.getRollFrameStore("a1_aggregate", "test1", StringConstants.HDFS)
    val outStoreLocator = ErStoreLocator(name = "a1_aggregate", namespace = "test1", storeType = StringConstants.HDFS)
    val outStore1 = ErStore(storeLocator = outStoreLocator, partitions = Array(
      ErPartition(id = 0, storeLocator = outStoreLocator, processor = ta.localNode0)))

    val rf = ctx.load(inStore)
    println(System.currentTimeMillis() - start)
    start = System.currentTimeMillis()
    val fieldCountLocal = fieldCount
    val schema = SchemaUtil.getDoubleSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

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

    val resultFb = FrameDB(outStore1, 0).readOne()
    TestCase.assertEquals(resultFb.readDouble(0, 0), rowCount * 3, TestAssets.DELTA)
  }

  @Test
  def testMapBatch(): Unit = {
    val input = ta.loadCache(ta.getRollFrameStore("a1", "test1", StringConstants.FILE))
    val output = ta.getRollFrameStore("a1map", "test1", StringConstants.CACHE)
    val rf = ctx.load(input)
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
    val input = ta.getRollFrameStore("a1", "test1")
    val output = ta.getRollFrameStore("a1_reduce", "test1", StringConstants.CACHE)
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
    val fb = FrameDB(input, 0).readOne()
    val fb1 = FrameDB(output, 0).readOne()
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
    var start = System.currentTimeMillis()
    val inStore = ta.getRollFrameStore("a1", "test1", StringConstants.FILE)
    val rf = ctx.load(inStore)
    println(System.currentTimeMillis() - start)
    start = System.currentTimeMillis()
    val colsCount = fieldCount
    val schema = SchemaUtil.getDoubleSchema(colsCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

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
    })
    println(System.currentTimeMillis() - start)
    val aggregateFb = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/a1_aggregate/0").readOne()
    TestCase.assertEquals(aggregateFb.readDouble(0, 0), inStore.partitions.length * rowCount, TestAssets.DELTA)
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
    var start = System.currentTimeMillis()
    val rf = ctx.load(ta.getRollFrameStore("a1", "test1"))
    println(System.currentTimeMillis() - start)
    start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = SchemaUtil.getDoubleSchema(fieldCount)
    /*    val output1 = RfStore("r1","test1",1, storeType = FrameDB.CACHE)
        val output2 = RfStore("r2","test1",1, storeType = FrameDB.CACHE)
        val output3 = RfStore("r3","test1",1, storeType = FrameDB.CACHE)*/


    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

    val output1 = rf.aggregate(zeroValue, { (x, y) =>
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
      for (i <- 0 until fieldCount) {
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
   * a demo realizing matrix multiplication by using map function
   */
  @Test
  def testMatMulV1ByMpBatch(): Unit = {
    val input = ta.loadCache(ta.getRollFrameStore("a1", "test1", StringConstants.FILE))
    val output = ta.getRollFrameStore("a1Matrix", "test1", StringConstants.CACHE)
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
   * a demo realizing matrix multiplication using ColumnVectors
   */
  @Test
  def testMatMulV1(): Unit = {
    val input = ta.loadCache(ta.getRollFrameStore("a1", "test1", StringConstants.FILE))
    val output = ta.getRollFrameStore("a1Matrix1", "test1", StringConstants.CACHE)
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
      val resCF = new ColumnFrame(FrameDB(output, i).readOne(), matrixCols)
      TestCase.assertEquals(resCF.fb.rowCount, rowCount * matrixCols)
      println(s"partition $i, value = ${resCF.read(0, 0)}")
    }
  }
}

class RollFrameClusterTests extends RollFrameTests {
  @Before
  override def setup(): Unit = {
    ctx = ta.getRfContext(false)
    HdfsBlockAdapter.fastSetLocal()
    ta.setMode("local")
    try{
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