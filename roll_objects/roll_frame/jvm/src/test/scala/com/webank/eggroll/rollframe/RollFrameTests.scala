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

import com.webank.eggroll.core.io.adapter.BlockDeviceAdapter
import com.webank.eggroll.core.meta.{ErPartition, ErProcessor, ErStore, ErStoreLocator}
import com.webank.eggroll.format._
import org.junit.{Before, Test}

class RollFrameTests {
  private val testAssets = TestAssets
  private val clusterManager = testAssets.clusterManager
  @Before
  def setup():Unit = {
    testAssets.clusterManager.startServerCluster(nodeId = 0)
  }

  @Test
  def testMapBatch(): Unit = {
    val input = clusterManager.getRollFrameStore("a1","test1")
    val outputStoreLocator = input.storeLocator.copy(name = "b1map")
    val output = input.copy(storeLocator = outputStoreLocator, partitions = input.partitions.map(p => p.copy(storeLocator = outputStoreLocator)))

    println(s"output: ${output}")
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

    val result = new RollFrameClientMode(output)
      .mapBatch{ fb =>
        assert(fb.readDouble(0,1) == 0.1)
        assert(fb.readDouble(1,0) == 1.0)
        assert(fb.readDouble(1,1) == 1.1)
        fb
      }

    result
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
  private def getSchema(fieldCount:Int):String = {
    val sb = new StringBuilder
    sb.append("""{
                 "fields": [""")
    (0 until fieldCount).foreach{i =>
      if(i > 0) {
        sb.append(",")
      }
      sb.append(s"""{"name":"double$i", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}""")
    }
    sb.append("]}")
    sb.toString()
  }

  def loadCaches():Unit = {
    def loadCache(path: String): Unit = {
      val inputDB = FrameDB.file(path)
      val outputDB = FrameDB.cache(path)
      outputDB.writeAll(inputDB.readAll())
      outputDB.close()
      inputDB.close()
    }
    loadCache("/tmp/unittests/RollFrameTests/file/test1/a1/0")
    loadCache("/tmp/unittests/RollFrameTests/file/test1/a1/1")
  }

  @Test
  def testFastWriteData(): Unit = {
    def write(path: String): Unit = {
      val fieldCount = 100
      val rowCount = 10000 // total value count = rowCount * fbCount * fieldCount
      val fbCount = 10    // the num of batch
      val randomObj = new Random()
      val adapter = FrameDB.file(path)
      (0 until fbCount).foreach { i =>
        val fb = new FrameBatch(new FrameSchema(testAssets.getDoubleSchema(fieldCount)), rowCount)
        for {x <- 0 until fieldCount
             y <- 0 until rowCount} {
          fb.writeDouble(x, y, randomObj.nextDouble())
        }
        println(s"FrameBatch order: $i,row count: ${fb.rowCount}")
        adapter.append(fb)
      }
    }

    def read(path: String): Unit = {
      val adapter = FrameDB.file(path)
      var num = 0
      adapter.readAll().foreach(_ => num += 1)
      println(s"Batch count: $num")
      val oneFb = adapter.readOne()

      println(oneFb.rootVectors(0).valueCount)
      println(s"FrameBatch row count: ${oneFb.rowCount}")
      println(oneFb.readDouble(0,0))
      println(oneFb.readDouble(0,1))

    }
    write("/tmp/unittests/RollFrameTests/file/test1/b1/0")
    write("/tmp/unittests/RollFrameTests/file/test1/b1/1")
    write("/tmp/unittests/RollFrameTests/file/test1/b1/2")

    read("/tmp/unittests/RollFrameTests/file/test1/b1/0")
  }

  @Test
  def testColumnarWrite1000():Unit = {

    def pass(path:String):Unit = {
      val fieldCount = 10
      val schema = getSchema(fieldCount)
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
  def testRollFrameAggregateBatch1(): Unit = {
    var start = System.currentTimeMillis()
    val clusterManager = new ClusterManager
    //val ps = List(RfPartition(0,1), RfPartition(1,1))
    //val inStore = RfStore("a1", "test1", ps.size, ps)

    val storeLocator = ErStoreLocator(name = "a1", namespace = "test1", storeType = "file")
    //val inStore = ErStore(storeLocator = storeLocator, partitions = List(ErPartition(id = "0", storeLocator = storeLocator, node = ErServerNode())))
    val inStore = clusterManager.getRollFrameStore("a1", "test1")
    val rf = new RollFrameClientMode(inStore)
    //    loadCaches()
    println(System.currentTimeMillis() -start);start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = getSchema(fieldCount)
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
          //          x.writeDouble(f,0, fv.valueCount)
        }
      } catch {
        case t:Throwable => t.printStackTrace()
      }
      //      x.columnarVectors.foreach(_.valueCount(5))
      print("x.valueCount", x.rootVectors(0).valueCount)
      x
    }, {(a, b) =>
      for(i <- 0 until fieldCount) {
        a.writeDouble(i, 0, a.readDouble(i,0) + b.readDouble(i,0))
      }
      a
    })
    println(System.currentTimeMillis() - start)
  }
  @Test
  def testRollFrameAggregate(): Unit = {
    var start = System.currentTimeMillis()
    val clusterManager = new ClusterManager
    val rf = new RollFrameClientMode(clusterManager.getRollFrameStore("a1","test1"))
//    loadCaches()
    println(System.currentTimeMillis() -start);start = System.currentTimeMillis()
    val fieldCount = 1000
    val schema = getSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until 1000).foreach(i => zeroValue.writeDouble(i,0,0))

    rf.aggregate(zeroValue,{(x, y) =>
      try{
        for(f <- y.rootVectors.indices) {
          var sum = 0.0
          val fv = y.rootVectors(f)
          for(i <- 0 until fv.valueCount) {
            sum += fv.readDouble(i)
          }
          x.writeDouble(f,0, sum)
//          x.writeDouble(f,0, fv.valueCount)
        }
      } catch {
        case t:Throwable => t.printStackTrace()
      }
      //      x.columnarVectors.foreach(_.valueCount(5))
      print("x.valueCount", x.rootVectors(0).valueCount)
      x
    }, {(a, b) =>
      for(i <- 0 until fieldCount) {
        a.writeDouble(i, 0, a.readDouble(i,0) + b.readDouble(i,0))
      }
      a
    })
    println(System.currentTimeMillis() - start)
  }

  @Test
  def testRollFrameAggregateBy(): Unit = {
    var start = System.currentTimeMillis()
    val clusterManager = new ClusterManager
    val rf = new RollFrameClientMode(clusterManager.getRollFrameStore("a1","test1"))
    //    loadCaches()
    println(System.currentTimeMillis() -start);start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = getSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until fieldCount).foreach(i => zeroValue.writeDouble(i,0,0))

    rf.aggregate(zeroValue,{(x, y) =>
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
    }, byColumn = true, broadcastZeroValue = true, output = ErStore(ErStoreLocator("file", "r1", "test1")))
    println(System.currentTimeMillis() - start)
  }

  @Test
  def testRollFrameMulti(): Unit = {
    var start = System.currentTimeMillis()
    val clusterManager = new ClusterManager
    val rf = new RollFrameClientMode(clusterManager.getRollFrameStore("a1","test1"))
    //    loadCaches()
    println(System.currentTimeMillis() -start);start = System.currentTimeMillis()
    val fieldCount = 10
    val schema = getSchema(fieldCount)
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
