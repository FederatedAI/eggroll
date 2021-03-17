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
import com.webank.eggroll.core.meta.ErStore
import com.webank.eggroll.core.util.TimeUtils
import com.webank.eggroll.util.Logging
import com.webank.eggroll.format._
import com.webank.eggroll.util.SchemaUtil
import junit.framework.TestCase
import org.junit.{Before, Ignore, Test}

/**
 * all unit test run on local mode
 */
class RollFrameTests extends Logging {
  protected val ta: TestAssets.type = TestAssets
  val fieldCount: Int = 10
  protected val rowCount = 10000 // total value count = rowCount * fbCount * fieldCount
  protected val fbCount = 1 // the num of batch
  protected val supportTorch = true
  protected var ctx: RollFrameContext = _
  protected var inputStore: ErStore = _
  protected var inputTensorStore: ErStore = _
  protected val partitions_ = 2

  @Before
  def setup(): Unit = {
    ctx = ta.getRfContext()
    info(s"get RfContext property unsafe:${System.getProperty("arrow.enable_unsafe_memory_access")}")
    inputStore = ctx.createStore("test1", "a1", StringConstants.FILE, partitions_)
    inputTensorStore = ctx.createStore("test1", "t1", StringConstants.FILE, partitions_)
    printContextMessage()
  }

  private def writeTensorFb(adapter: FrameStore): Unit = {
    val randomObj = new Random()
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.oneDoubleFieldSchema), fieldCount * rowCount)
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

  /**
   * the other test methods depend on these frameBatches
   */
  @Test
  def testCreateDataStore(): Unit = {
    RollFrameContext.printSession(ctx.session)
    inputStore.partitions.indices.foreach { i =>
      write(FrameStore(inputStore, i)) //    create file FrameBatch
      writeTensorFb(FrameStore(inputTensorStore, i)) //    create FrameBatch with one column
    }
    //    read(FrameStore.file("/tmp/unittests/RollFrameTests/file/test1/a1/0"))
    //    read(FrameStore.file("/tmp/unittests/RollFrameTests/file/test1/a1/1"))
  }

  @Test
  def testRunPythonCode(): Unit = {
    val inputCacheStore = ctx.combineDoubleFbs(ctx.dumpCache(inputTensorStore))
    val rf = ctx.load(inputCacheStore)
    val code: String =
      """
        |import numpy as np
        |import torch
        |data = torch.from_numpy(_input_data)
        |print(data.type())
        |a = _world_size
        |b = _rank
        |print(_parameters)
        |print(_parameters.dtype)
        |
        |_state = 0
        |_result = torch.ones(3).numpy()
        |_result = _result.astype(np.float64)
        |""".stripMargin

    val parameters = Array(1.0, 2.0)
    (0 until 10).foreach { i =>
      val outputStore = ctx.forkStore(inputCacheStore, "test" + i, "out1", StringConstants.CACHE)
      rf.runPythonDistributed(code, fieldCount, parameters, nonCopy = true, output = outputStore)
      val res = FrameStore(outputStore, 0).readOne()
      assert(res.readDouble(0, 0) == 1.0)
    }
  }

  @Test
  def testLoadRollFrame(): Unit = {
    val rf = ctx.load(ctx.dumpCache(inputStore))
    val output = ctx.createStore("test", "rrr1", StringConstants.FILE, 1)
    rf.mapBatch(fb => {
      TestCase.assertNotNull(fb.isEmpty)
      fb
    }, output)
  }

  @Test
  def testCheckStoreExists(): Unit = {
    val cacheStore = ctx.dumpCache(inputStore)
    val res = ctx.frameTransfer.Roll.checkStoreExists(cacheStore.storeLocator)
    TestCase.assertEquals(res, true)
    val cacheStore1 = ctx.createStore("test1", "aa1", StringConstants.CACHE, partitions_)
    val res1 = ctx.frameTransfer.Roll.checkStoreExists(cacheStore1.storeLocator)
    TestCase.assertEquals(res1, false)
  }

  @Test
  def testNetworkToJvm(): Unit = {
    // 1.write to network and continue write to file
    val fieldCount = 1000
    val rowCount = 10000
    val networkStore = ctx.createStore("test1", "a1", StringConstants.NETWORK, partitions_)
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), rowCount)
    networkStore.partitions.indices.foreach { i =>
      new Thread() {
        override def run(): Unit = {
          FrameStore(networkStore, i).append(fb)
        }
      }.start()
    }
    Thread.sleep(2000)
    val start = System.currentTimeMillis()
    val cacheStore = ctx.dumpCache(networkStore)
    println(s"run time = ${System.currentTimeMillis() - start}")
    ctx.load(cacheStore).mapBatch({ fb =>
      TestCase.assertNotNull(fb.isEmpty)
      fb
    })
  }

  @Test
  def testBroadcast(): Unit = {
    val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), rowCount)
    ctx.frameTransfer.broadcast("a", zeroValue)
    ctx.frameTransfer.broadcast("b", zeroValue)
    TestCase.assertNotNull(JvmFrameStore.checkFrameBatch("a"))
    TestCase.assertNotNull(JvmFrameStore.checkFrameBatch("b"))
  }

  @Test
  def testPull(): Unit = {
    val cacheStore = ctx.dumpCache(inputStore)
    cacheStore.partitions.foreach(println)
    val path = FrameStore.getStorePath(cacheStore.partitions(0))
    println(path)
    val fb = ctx.frameTransfer.Roll.pull(path).next()
    TestCase.assertNotNull(fb.isEmpty)
  }

  @Test
  def testGetAvailablePort(): Unit = {
    TestCase.assertTrue(ctx.frameTransfer.Roll.getAvailablePort >= HttpUtil.ORIGIN_PORT)
  }

  /**
   * a demo of slice a FrameBatch by rows and broadcast to the cluster to run aggregation
   */
  @Test
  def testSliceByRowAndAggregate(): Unit = {
    // 1. FrameBatch on root server
    val fb = FrameStore(inputStore, 0).readOne()
    // 2. distribute and load to caches
    val networkStore = ctx.forkStore(inputStore, "test1", "a1", StringConstants.NETWORK)
    val partitionsNum = networkStore.partitions.length
    val sliceRowCount = (partitionsNum + fb.rowCount - 1) / partitionsNum
    (0 until partitionsNum).foreach(i =>
      FrameStore(networkStore, i).append(fb.sliceRealByRow(i * sliceRowCount, sliceRowCount))
    )

    // 3. aggregate operation
    val rf = ctx.load(networkStore)
    val start = System.currentTimeMillis()
    val fieldCount = fb.fieldCount
    val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), 1)
    zeroValue.initZero()
    val outStore = ctx.createStore("test1", "a1Sr", StringConstants.CACHE, totalPartitions = 1)
    rf.simpleAggregate(zeroValue, { (x, y) =>
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
  def testMapBatch(): Unit = {
    val rf = ctx.load(ctx.dumpCache(inputStore))
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
    }).mapBatch { fb =>
      TestCase.assertEquals(fb.readDouble(0, 1), 0.1, TestAssets.DELTA)
      TestCase.assertEquals(fb.readDouble(1, 0), 1.0, TestAssets.DELTA)
      TestCase.assertEquals(fb.readDouble(1, 1), 1.1, TestAssets.DELTA)
      fb
    }
    println(s"run time = ${System.currentTimeMillis() - start} ms")
  }

  @Test
  def testMapBatch1(): Unit = {
    val input = ctx.dumpCache(inputStore)
    val output = ctx.forkStore(input, "test1", "a1map1", StringConstants.CACHE)
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
  def testSimpleAggregate(): Unit = {
    var start = System.currentTimeMillis()
    val inStore = ctx.dumpCache(inputStore)
    val outStore = ctx.createStore("a1_aggregate", "test1", StringConstants.CACHE, totalPartitions = 1)
    val rf = ctx.load(inStore)
    start = System.currentTimeMillis()
    val fieldCountLocal = fieldCount
    val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), 1)
    zeroValue.initZero()
    rf.simpleAggregate(zeroValue, { (x, y) =>
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
          throw t
      }
      x
    }, { (a, b) =>
      for (i <- 0 until fieldCountLocal) {
        a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    }, output = outStore)
    println(System.currentTimeMillis() - start)
    val resultFb = FrameStore(outStore, 0).readOne()
    TestCase.assertEquals(resultFb.readDouble(0, 0), inStore.partitions.length * rowCount, TestAssets.DELTA)
  }

  @Test
  def testReduceBatch(): Unit = {
    val input = ctx.dumpCache(inputStore)
    val fb = FrameStore(input, 0).readOne()
    println(fb.rootVectors.head.getDataBufferAddress)
    val output = ctx.createStore("test1", "a1_reduce", StringConstants.FILE, 1)
    val start = System.currentTimeMillis()
    ctx.load(input).reduce({ (x, y) =>
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
    }, output = output)
    println(s"Time = ${System.currentTimeMillis() - start} ms")
    val origin_fb = FrameStore(input, 0).readOne()
    val fb1 = FrameStore(output, 0).readOne()
    // todo: bug,address change
    print(origin_fb.rootVectors.head.getDataBufferAddress)
    TestCase.assertEquals(origin_fb.fieldCount, fb1.fieldCount)
    TestCase.assertEquals(origin_fb.rowCount, fb1.rowCount)
  }

  @Test
  def testMapAndReduce(): Unit = {
    val input = ctx.dumpCache(inputStore)
    val output = ctx.createStore("test1", "a1_reduce", StringConstants.CACHE, 1)
    val start = System.currentTimeMillis()
    val cols = fieldCount
    val rows = rowCount
    val mapTmp = ctx.load(input).mapBatch(fb => {
      val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(cols)), rows)
      (0 until zeroValue.fieldCount).foreach { f =>
        (0 until zeroValue.rowCount).foreach { r =>
          zeroValue.writeDouble(f, r, fb.readDouble(f, r))
        }
      }
      zeroValue
    }).reduce({ (x, y) =>
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
    }, output = output)
    val f = FrameStore(output, 0).readOne()
    print(f.rowCount, f.fieldCount)
    println(s"time = ${System.currentTimeMillis() - start} ms")
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
  def testCombineDoubleFbs(): Unit = {
    val input = ctx.dumpCache(inputStore)
    val output = ctx.combineDoubleFbs(input)
    val fb_i = FrameStore(inputStore, 0).readOne()
    val fb_o = FrameStore(output, 0).readOne()
    assert(fb_i.rowCount * partitions_ == fb_o.rowCount)
  }

  @Test
  def testDenJvmData(): Unit = {
    val input = ctx.createStore("test1", "a1", StringConstants.CACHE, 4)
    ctx.load(input).genData(_ => {
      val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(10)), 1000)
      zeroValue.initZero()
      Iterator(zeroValue)
    })
    val fb = FrameStore(input, 0).readOne()
    TestCase.assertEquals(fb.fieldCount, 10)
    TestCase.assertEquals(fb.rowCount, 1000)
  }

  @Test
  def testMapCommand(): Unit = {
    val input = ctx.dumpCache(inputStore)
    val start = System.currentTimeMillis()
    ctx.load(input).mapCommand(f =>
      println(f.rowCount)
    )
    println(s"time:${System.currentTimeMillis() - start} ms")
  }

  @Test
  @Ignore("Only suit for one partition with one process's data store.")
  def testAllReduce(): Unit = {
    val input = ctx.dumpCache(inputStore)
    val output = ctx.forkStore(input, input.storeLocator.namespace, "all_reduce", StringConstants.CACHE)
    val start = System.currentTimeMillis()
    ctx.load(input).allReduce((a, b) => {
      (0 until a.fieldCount).foreach { f =>
        (0 until a.rowCount).foreach { r =>
          a.writeDouble(f, r, a.readDouble(f, r) + b.readDouble(f, r))
        }
      }
      a
    }, output)
    println(s"time:${System.currentTimeMillis() - start} ms")
    val stop = 0
  }

  @Test
  def testRelease(): Unit = {
    val store1 = ctx.createStore("test1", "a1", StringConstants.CACHE, 3)
    val store2 = ctx.createStore("test1", "a11", StringConstants.CACHE, 4)
    store1.partitions.indices.foreach { i =>
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(1000)), 50000)
      fb.initZero()
      FrameStore(store1, i).append(fb)
    }
    store2.partitions.indices.foreach { i =>
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(1000)), 50000)
      fb.initZero()
      FrameStore(store2, i).append(fb)
    }
    JvmFrameStore.printJvmFrameStore()
    ctx.frameTransfer.addExcludeStore("test1", "a1")
    //    Thread.sleep(2000)
    //    ctx.frameTransfer.deleteExcludeStore(store1)
    //    ctx.frameTransfer.addExcludeStore(store1)
    ctx.frameTransfer.releaseStore()
    Thread.sleep(2000)
  }

  @Test
  def testReleaseStoreByTransfer(): Unit = {
    val input = ctx.dumpCache(inputStore)
    val output = ctx.forkStore(input, "test1", "x1", StringConstants.CACHE)
    val start = System.currentTimeMillis()
    println(start)
    ctx.load(input).mapBatch(fb => {
      val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(1000)), 10000)
      zeroValue.initZero()
      zeroValue
    }, output)

    ctx.frameTransfer.addExcludeStore(input)
    Thread.sleep(2000)
    ctx.frameTransfer.releaseStore()
    Thread.sleep(2000)
  }

  @Test
  def testParallelAggregateByData(): Unit = {
    val input = ctx.dumpCache(inputStore)
    val output = ctx.createStore("test1", "r1", StringConstants.CACHE, 1)
    val cols = 1000
    val rows = 1000
    val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(cols)), rows)
    (0 until zeroValue.fieldCount).foreach { f =>
      (0 until zeroValue.rowCount).foreach { r =>
        zeroValue.writeDouble(f, r, 1)
      }
    }
    val out = ctx.load(input).aggregate(zeroValue, combOp = { (a, b) =>
      for (i <- 0 until a.fieldCount) {
        a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    },
      seqByColumnOp = { (zero, cb, _) =>
        (0 until cb.fieldCount).foreach(i =>
          if (cb.rootVectors(i) != null) {
            for (j <- 0 until zero.rowCount) {
              zero.writeDouble(i, j, 1 + zero.readDouble(i, j))
            }
          })
      }, broadcastZeroValue = true, output = output)

  }

  @Test
  def TestParallelAggregateByZero(): Unit = {
    val input = ctx.dumpCache(inputStore)
    val output = ctx.createStore("test1", "r1", StringConstants.CACHE, 1)
    val cols = fieldCount
    val rows = rowCount
    var start = System.currentTimeMillis()
    val zeroValue = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(cols)), rows)
    zeroValue.initZero()
    start = System.currentTimeMillis()
    val res = ctx.load(input).aggregate(zeroValue, combOp = { (a, b) =>
      for (i <- 0 until a.fieldCount) {
        a.writeDouble(i, 0, a.readDouble(i, 0) + b.readDouble(i, 0))
      }
      a
    }, commParaOp = { (zero, cb) =>
      val nodeIndexMap = Array.fill(10 + 1)(-1)
      val predict = Array.fill(cb.rowCount)((-1, 0))
      Map("nodeIndexMap" -> nodeIndexMap, "predict" -> predict)
    }, seqByColumnOp = { (zero, cb, pa) =>
      (0 until zero.fieldCount).foreach { f =>
        if (zero.rootVectors(f) != null) {
          var sum = 0.0
          (0 until cb.rowCount).foreach { r =>
            sum += cb.readDouble(f, r)
          }
          for (i <- 0 until zero.rowCount) {
            zero.writeDouble(f, i, 1)
          }
        }
      }
      val predict = pa("predict").asInstanceOf[Array[(Int, Int)]]
      val a = predict(0)
    }, seqByZeroValue = true, broadcastZeroValue = true, output = output)

    println(s"time = ${System.currentTimeMillis() - start} ms")
    Thread.sleep(3000)
  }

  @Test
  def testRollFrameMulti(): Unit = {
    val rf = ctx.load(ctx.dumpCache(inputStore))
    val start = System.currentTimeMillis()
    val colsCount = fieldCount
    val schema = SchemaUtil.getDoubleSchema(colsCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    zeroValue.initZero()

    rf.simpleAggregate(zeroValue, { (x, y) =>
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
        rf.simpleAggregate(zeroValue, (x, _) => x, (a, _) => a)
        System.currentTimeMillis() - start
      }
    })

    val result1 = future1.get()

    val future2 = pool.submit(new Callable[Long] {
      override def call(): Long = {
        val start = System.currentTimeMillis()
        rf.simpleAggregate(zeroValue, (x, _) => x, (a, _) => a)
        System.currentTimeMillis() - start
      }
    })
    val result2 = future2.get()
    println(System.currentTimeMillis() - start, result1, result2)
  }
}
