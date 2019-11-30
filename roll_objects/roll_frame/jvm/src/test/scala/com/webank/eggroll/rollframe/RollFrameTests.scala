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

import java.util.Random
import java.util.concurrent.{Callable, Executors}

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.io.adapter.{BlockDeviceAdapter, HdfsBlockAdapter}
import com.webank.eggroll.core.meta.{ErPartition, ErProcessor, ErStore, ErStoreLocator}
import com.webank.eggroll.format._
import org.junit.{Before, Test}

class RollFrameTests {
  private val testAssets = TestAssets
  private val clusterManager = testAssets.clusterManager
  @Before
  def setup():Unit = {
    HdfsBlockAdapter.fastSetLocal()
    testAssets.clusterManager.startServerCluster()
  }

  @Test
  def testCreateFrameBatch(): Unit = {
    val fieldCount = 10
    val rowCount = 100 // total value count = rowCount * fbCount * fieldCount
    val fbCount = 1 // the num of batch

    def write(adapter: FrameDB): Unit = {
      val randomObj = new Random()
      (0 until fbCount).foreach { i =>
        val fb = new FrameBatch(new FrameSchema(testAssets.getDoubleSchema(fieldCount)), rowCount)
        for {x <- 0 until fieldCount
             y <- 0 until rowCount} {
          fb.writeDouble(x, y, randomObj.nextDouble())
        }
        println(s"FrameBatch order: $i,row count: ${fb.rowCount}")
        adapter.append(fb)
      }
      adapter.close()
    }

    def read(adapter: FrameDB): Unit = {
      var num = 0
      adapter.readAll().foreach(_ => num += 1)
      val oneFb = adapter.readOne()
      adapter.close()
      assert(fbCount == num)
      assert(fieldCount == oneFb.fieldCount)
      assert(rowCount == oneFb.rowCount)
    }

    val output = ErStore(storeLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.FILE))

    write(FrameDB(output,0))
    write(FrameDB(output,1))
    read(FrameDB(output,0))
    read(FrameDB(output,1))

    write(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/0"))
    write(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/1"))
    read(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/0"))
    read(FrameDB.hdfs("/tmp/unittests/RollFrameTests/hdfs/test1/a1/1"))
  }

  @Test
  def testHdfsToJvm(): Unit ={
    val storeLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.HDFS)
    val input = ErStore(storeLocator = storeLocator,
      partitions = Array(
        ErPartition(id = 0, storeLocator = storeLocator, processor = clusterManager.localNode0),
        ErPartition(id = 1, storeLocator = storeLocator, processor = clusterManager.localNode1)))

    val outputStoreLocator = ErStoreLocator(name = "a1map", namespace = "test1", storeType = StringConstants.CACHE)
    val output = ErStore(storeLocator = outputStoreLocator,
      partitions = Array(
        ErPartition(id = 0, storeLocator = outputStoreLocator, processor = clusterManager.localNode0),
        ErPartition(id = 1, storeLocator = outputStoreLocator, processor = clusterManager.localNode1)))

    val rf = new RollFrameClientMode(input)
    rf.mapBatch({ cb =>
      cb
    },output = output)

    val fd1 = FrameDB(input,0).readOne()
    val fd2 = FrameDB(output, 0).readOne() // take first partition to assert
    assert(fd1.readDouble(0,0) == fd2.readDouble(0,0))
  }

  @Test
  def testMapBatchWithHdfs(): Unit = {
    val storeLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.HDFS)
    val input = ErStore(storeLocator = storeLocator,
      partitions = Array(
        ErPartition(id = 0, storeLocator = storeLocator, processor = clusterManager.localNode0),
        ErPartition(id = 1, storeLocator = storeLocator, processor = clusterManager.localNode1)))

    val outputStoreLocator = input.storeLocator.copy(name = "a1map")
    val output = input.copy(storeLocator = outputStoreLocator,
      partitions = Array(
        ErPartition(id = 0, storeLocator = outputStoreLocator, processor = clusterManager.localNode0),
        ErPartition(id = 1, storeLocator = outputStoreLocator, processor = clusterManager.localNode1)))

    val rf = new RollFrameClientMode(input)
    rf.mapBatch({ cb =>
      val mapRf = new FrameBatch(cb.rootSchema)
      for (column <- 0 until mapRf.fieldCount) {
        for (row <- 0 until mapRf.rowCount) {
          mapRf.writeDouble(column, row, row)
        }
      }
      mapRf
    },output=output)

    val mapFb0 = FrameDB(output, 0).readOne() // take first partition to assert
    assert(mapFb0.readDouble(0, 0) == 0.0)
    assert(mapFb0.readDouble(0, 1) == 1.0)
    val mapFb1 = FrameDB(output, 1).readOne() // take second partition to assert
    assert(mapFb1.readDouble(2, 10) == 10.0)
    assert(mapFb1.readDouble(2, 20) == 20.0)
  }

  @Test
  def testAggregateWithHdfs(): Unit = {
    var start = System.currentTimeMillis()
    val inStoreLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = StringConstants.HDFS)
    val inStore = ErStore(storeLocator = inStoreLocator,
      partitions = Array(
        ErPartition(id = 0, storeLocator = inStoreLocator, processor = clusterManager.localNode0),
        ErPartition(id = 1, storeLocator = inStoreLocator, processor = clusterManager.localNode1)))

    val outStoreLocator = ErStoreLocator(name = "a1_aggregate", namespace = "test1", storeType = StringConstants.HDFS)
    val outStore = ErStore(storeLocator = outStoreLocator)
    val outStore1 = ErStore(storeLocator = outStoreLocator,partitions = Array(
      ErPartition(id = 0, storeLocator = inStoreLocator, processor = clusterManager.localNode0)))

    val rf = new RollFrameClientMode(inStore)
    //    loadCaches()
    println(System.currentTimeMillis() - start)
    start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = TestAssets.getDoubleSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i, 0, 0))

    rf.aggregate(zeroValue, { (x, y) =>
      try {
        for (f <- y.rootVectors.indices) {
          val sum = y.rowCount
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
    },broadcastZeroValue = true, output = outStore)
    println(System.currentTimeMillis() - start)

    val resultFb = FrameDB(outStore,0).readOne()
    assert(resultFb.readDouble(0, 0) == 200)
  }

  @Test
  def testMapBatch(): Unit = {
    val input = clusterManager.getRollFrameStore("a1", "test1")
    val outputStoreLocator = input.storeLocator.copy(name = "a1map")
    val output = input.copy(storeLocator = outputStoreLocator, partitions = input.partitions.map(p => p.copy(storeLocator = outputStoreLocator)))

    val rf = new RollFrameClientMode(input)
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
      batch.writeDouble(0, 0, 0.0 + cb.readDouble(0,0))
      batch.writeDouble(0, 1, 0.1)
      batch.writeDouble(1, 0, 1.0)
      batch.writeDouble(1, 1, 1.1)
      batch
    }, output)

    new RollFrameClientMode(output)
      .mapBatch { fb =>
        assert(fb.readDouble(0, 1) == 0.1)
        assert(fb.readDouble(1, 0) == 1.0)
        assert(fb.readDouble(1, 1) == 1.1)
        fb
      }
  }

  @Test
  def testReduceBatch(): Unit = {
    val clusterManager = new ClusterManager
    val input = clusterManager.getRollFrameStore("a1","test1")
    val outputStoreLocator = input.storeLocator.copy(name = "b1reduce")
    val output = input.copy(storeLocator = outputStoreLocator, partitions = input.partitions.map(p => p.copy(storeLocator = outputStoreLocator)))
    val rf = new RollFrameClientMode(input)
    rf.reduce{(x, y) =>
      try{
        for(f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          for(i <- 0 until fv.valueCount) {
            sum += fv.readDouble(i)
          }
          x.writeDouble(f,0, sum)
        }
      } catch {
        case t:Throwable => t.printStackTrace()
      }
      x
    }
    FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/a1/0")
      .readAll().foreach(fb => assert(fb.rowCount > 0))
  }

  def loadCaches():Unit = {
    TestAssets.loadCache("/tmp/unittests/RollFrameTests/file/test1/a1/0",StringConstants.FILE)
    TestAssets.loadCache("/tmp/unittests/RollFrameTests/file/test1/a1/1",StringConstants.FILE)
  }

  @Test
  def testColumnarWrite1000():Unit = {

    def pass(path:String):Unit = {
      val fieldCount = 10
      val schema = TestAssets.getDoubleSchema(fieldCount)
      val cw = new FrameWriter(new FrameSchema(schema), BlockDeviceAdapter.file(path))
      val valueCount = 500*100 / 2
      val batchSize = 500*100 / 2
      cw.write(valueCount, batchSize,
        (fid, cv) => (0 until batchSize).foreach(
          //        n => cv.writeDouble(n, fid * valueCount + n * 0.5)
          n => cv.writeDouble(n, fid  * new Random().nextDouble())
        )
      )
      cw.close()
    }
    pass("/tmp/unittests/RollFrameTests/file/test1/a1/0")
    pass("/tmp/unittests/RollFrameTests/file/test1/a1/1")

  }
  @Test
  def testRollFrameAggregateBatch(): Unit = {
    var start = System.currentTimeMillis()
    val clusterManager = new ClusterManager

    val storeLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = "file")
    //val inStore = ErStore(storeLocator = storeLocator, partitions = List(ErPartition(id = "0", storeLocator = storeLocator, node = ErServerNode())))
    val inStore = clusterManager.getRollFrameStore("a1", "test1")
    val rf = new RollFrameClientMode(inStore)
    //    loadCaches()
    println(System.currentTimeMillis() -start);start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = TestAssets.getDoubleSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i,0,0))

    rf.aggregate(zeroValue,{(x, y) =>
      try{
        for(f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          for(i <- 0 until fv.valueCount) {
            sum += fv.readDouble(i)
          }
          x.writeDouble(f,0, sum)
        }
      } catch {
        case t:Throwable => t.printStackTrace()
      }
      x
    }, {(a, b) =>
      for(i <- 0 until fieldCount) {
        a.writeDouble(i, 0, a.readDouble(i,0) + b.readDouble(i,0))
      }
      a
    })
    println(System.currentTimeMillis() - start)
    val aggregateFb = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/aggregate/0").readOne()
    val aggregateFb1 = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/aggregate/1").readOne()

    println(aggregateFb.readDouble(0, 0))
    println(aggregateFb1.readDouble(0, 0))
  }

  @Test
  def testRollFrameAggregateBy(): Unit = {
    var start = System.currentTimeMillis()
    val clusterManager = new ClusterManager
    val rf = new RollFrameClientMode(clusterManager.getRollFrameStore("a1","test1"))
    //    loadCaches()
    println(System.currentTimeMillis() -start);start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = TestAssets.getDoubleSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i,0,0))

    rf.aggregate(zeroValue,{(x, y) =>
      try{
        for(f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          if(fv != null){
            for(i <- 0 until fv.valueCount) {
              sum += 1
            }
            x.writeDouble(f,0, sum)
            //          x.writeDouble(f,0, fv.valueCount)
          }

        }
      } catch {
        case t:Throwable => t.printStackTrace()
      }
      //      x.columnarVectors.foreach(_.valueCount(5))
      print("x.valueCount", x.rootVectors(0).valueCount)
      x
    }, {(a, b) =>
      for(i <- 0 until fieldCount) {
        if(b.rootVectors(i) != null) a.writeDouble(i, 0, a.readDouble(i,0) + b.readDouble(i,0))
      }
      a
    }, byColumn = true, broadcastZeroValue = true, output = ErStore(ErStoreLocator("file", "test1", "r1byC")))
    println(System.currentTimeMillis() - start)
    val aggregateFb = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/r1byC/0").readOne()
    val aggregateFb1 = FrameDB.file("/tmp/unittests/RollFrameTests/file/test1/r1byC/1").readOne()
//
    println(aggregateFb.readDouble(0, 0))
    println(aggregateFb1.readDouble(0, 0))
  }

  @Test
  def testRollFrameMulti(): Unit = {
    var start = System.currentTimeMillis()
    val clusterManager = new ClusterManager
    val rf = new RollFrameClientMode(clusterManager.getRollFrameStore("a1","test1"))
    //    loadCaches()
    println(System.currentTimeMillis() -start);start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = TestAssets.getDoubleSchema(fieldCount)
/*    val output1 = RfStore("r1","test1",1, storeType = FrameDB.CACHE)
    val output2 = RfStore("r2","test1",1, storeType = FrameDB.CACHE)
    val output3 = RfStore("r3","test1",1, storeType = FrameDB.CACHE)*/


    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i,0,0))

    val output1 = rf.aggregate(zeroValue,{(x, y) =>
      try{
        for(f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          if(fv != null){
            for(i <- 0 until fv.valueCount) {
              sum += fv.readDouble(i)
            }
            x.writeDouble(f,0, sum)
            //          x.writeDouble(f,0, fv.valueCount)
          }
        }
      } catch {
        case t:Throwable => t.printStackTrace()
      }
      //      x.columnarVectors.foreach(_.valueCount(5))
      print("x.valueCount", x.rootVectors(0).valueCount)
      x
    }, {(a, b) =>
      for(i <- 0 until fieldCount) {
        if(b.rootVectors(i) != null) a.writeDouble(i, 0, a.readDouble(i,0) + b.readDouble(i,0))
      }
      a
    },byColumn = true, broadcastZeroValue = true)
    val pool = Executors.newFixedThreadPool(2)
    val future1 = pool.submit(new Callable[Long] {
      override def call(): Long = {
        val start = System.currentTimeMillis()
        new RollFrameClientMode(output1.store).aggregate(zeroValue, (x, _) => x, (a, _) => a)
        System.currentTimeMillis() - start
      }
    })

    val result1 = future1.get()

    val future2 = pool.submit(new Callable[Long] {
      override def call(): Long = {
        val start = System.currentTimeMillis()
        new RollFrameClientMode(output1.store).aggregate(zeroValue, (x, _)=>x, (a, _) => a)
        System.currentTimeMillis() - start
      }
    })
    val result2 = future2.get()
    println(System.currentTimeMillis() - start, result1, result2)
  }
}
