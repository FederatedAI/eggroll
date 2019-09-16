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

import java.util.Random

import com.webank.eggroll.blockstore.BlockStoreAdapter
import com.webank.eggroll.command.{CommandService, GrpcCommandService}
import com.webank.eggroll.format._
import com.webank.eggroll.transfer.GrpcTransferService
import io.grpc.ServerBuilder
import org.junit.{Before, Test}

class RollFrameTests {

  @Before
  def setup():Unit = {
    CommandService.register("com.webank.eggroll.rollframe.EggFrame.runTask",
      List(classOf[RollFrameGrpc.Task]),classOf[RollFrameGrpc.TaskResult])
  }
  @Test
  def testColumnarReadWrite():Unit = {
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
    val path = "./tmp/unittests/RollFrameTests/testColumnarWrite/0"
    val cw = new FrameWriter(schema, path)
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
    for( cb <- cr.getColumnarBatches){
      for( i <- 0 until fieldCount){
        val cv = cb.columnarVectors(i)
        for(n <- 0 until cv.valueCount) {
          println(i, cv.readDouble(n))
        }
        println("=============")
      }
      println("**********")
    }
    cr.close()
  }

  @Test
  def tesEggFrameReadWrite():Unit = {
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
    val path = "./tmp/unittests/RollFrameTests/testColumnarWrite/0"
    val cw = new FrameWriter(schema, path)
    val valueCount = 10
    val fieldCount = 3
    val batchSize = 5
    cw.write(valueCount, batchSize,
      (fid, cv) => (0 until valueCount).foreach(
        n => cv.writeDouble(n, fid * valueCount + n * 0.5)
      )
    )
    cw.close()
    val path2 = "./tmp/unittests/RollFrameTests/testColumnarWrite/1"

    val opts = Map("path" -> path2)
    val cr = new FrameReader(path)
//    for( cb <- cr.getColumnarBatches){
//      eggFrame.write(cb, opts)
//    }
    val adapter = FrameStoreAdapter(opts)
    adapter.writeAll(cr.getColumnarBatches)
    cr.close()
    adapter.close()
    val adapter2 = BlockStoreAdapter.create(opts)
    for( cb <- adapter.readAll()){
      for( i <- 0 until fieldCount){
        val cv = cb.columnarVectors(i)
        for(n <- 0 until cv.valueCount) {
          println(i, cv.readDouble(n))
        }
        println("=============")
      }
      println("**********")
    }
    adapter2.close()
  }
  @Test
  def testEggFrameServer(): Unit = {
    ServerBuilder.forPort(20101).addService(new GrpcCommandService()).build.start.awaitTermination()
  }
  @Test
  def testRollFrameServer(): Unit = {
    ServerBuilder.forPort(20100).addService(new GrpcCommandService()).build.start.awaitTermination()
  }

  @Test
  def testRollFrame(): Unit = {
    ServerBuilder.forPort(20101).addService(new GrpcCommandService()).build.start
    ServerBuilder.forPort(20100).addService(new GrpcCommandService()).build.start
    val clusterManager = new ClusterManager
    val rf = new RollFrameService(clusterManager.getRollFrameStore("a1","test1"))
    rf.mapBatches{cb =>
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
    }
  }
  @Test
  def testRollFrameReduce(): Unit = {
    val sb = ServerBuilder.forPort(20101)
    sb.addService(new GrpcCommandService())
    sb.addService(new GrpcTransferService).build.start
    val sb2 = ServerBuilder.forPort(20100)
    sb2.addService(new GrpcCommandService())
    sb2.addService(new GrpcTransferService()).build.start
    val clusterManager = new ClusterManager
    val rf = new RollFrameService(clusterManager.getRollFrameStore("a1","test1"))
    rf.reduce{(x, y) =>
      try{
        val schema =
          """
        {
          "fields": [
          {"name":"double1", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}},
          {"name":"double2", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}}
          ]
        }
      """.stripMargin
        for(f <- 0 until y.columnarVectors.size) {
          var sum = 0.0
          val fv = y.columnarVectors(f)
          for(i <- 0 until fv.valueCount) {
            sum += fv.readDouble(i)
          }
          x.writeDouble(f,0, sum)
        }
      } catch {
        case t:Throwable => t.printStackTrace()
      }
//      x.columnarVectors.foreach(_.valueCount(5))
      print("x.valueCount", x.columnarVectors(0).valueCount)
      x
    }
  }
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

  private def loadCaches():Unit = {
    def loadCache(path: String): Unit = {
      val inputStore = FrameStoreAdapter(Map("path" -> path))
      val outputStore = FrameStoreAdapter(Map("path" -> path, "type"->"jvm"))
      outputStore.writeAll(inputStore.readAll())
      outputStore.close()
      inputStore.close()
    }
    loadCache("./tmp/unittests/RollFrameTests/filedb/test1/a1/0")
    loadCache("./tmp/unittests/RollFrameTests/filedb/test1/a1/1")

  }
  @Test
  def testColumnarWrite1000():Unit = {

    def pass(path:String) = {
      val fieldCount = 1000
      val schema = getSchema(fieldCount)
      val cw = new FrameWriter(schema, path)
      val valueCount = 500*1000 / 2
      val batchSize = 500*1000 / 20
      cw.write(valueCount, batchSize,
        (fid, cv) => (0 until batchSize).foreach(
          //        n => cv.writeDouble(n, fid * valueCount + n * 0.5)
          n => cv.writeDouble(n, fid  * new Random().nextDouble())
        )
      )
      cw.close()
    }
    pass("./tmp/unittests/RollFrameTests/filedb/test1/a1/0")
    pass("./tmp/unittests/RollFrameTests/filedb/test1/a1/1")


  }

  @Test
  def testRollFrameAggregate(): Unit = {
    var start = System.currentTimeMillis()
    val sb = ServerBuilder.forPort(20101)
    sb.addService(new GrpcCommandService())
    sb.addService(new GrpcTransferService).build.start
    val sb2 = ServerBuilder.forPort(20100)
    sb2.addService(new GrpcCommandService())
    sb2.addService(new GrpcTransferService()).build.start
    val clusterManager = new ClusterManager
    val rf = new RollFrameService(clusterManager.getRollFrameStore("a1","test1"))
//    loadCaches()
    println(System.currentTimeMillis() -start);start = System.currentTimeMillis()
    val fieldCount = 1000
    val schema = getSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    (0 until 1000).foreach(i => zeroValue.writeDouble(i,0,0))

    rf.aggregate(zeroValue,{(x, y) =>
      try{
        for(f <- 0 until y.columnarVectors.size) {
          var sum = 0.0
          val fv = y.columnarVectors(f)
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
      print("x.valueCount", x.columnarVectors(0).valueCount)
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
  def testFrameDataType():Unit = {
    val schema =
      """
        {
          "fields": [
          {"name":"double1", "type": {"name" : "floatingpoint","precision" : "DOUBLE"}},
          {"name":"long1", "type": {"name" : "int","bitWidth" : 64,"isSigned":true}},
          {"name":"longarray1", "type": {"name" : "fixedsizelist","listSize" : 10}, "children":[{"name":"$data$", "type": {"name" : "int","bitWidth" : 64,"isSigned":true}}]}
          ]
        }
      """.stripMargin

    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    zeroValue.writeDouble(0, 0, 1.2)
    zeroValue.writeLong(1, 0, 22)
    val arr = zeroValue.getValueVector(2, 0)
    (0 until 10).foreach( i=> arr.writeLong(i, i * 100))
    val outputStore = FrameStoreAdapter(Map("path" -> "./tmp/unittests/RollFrameTests/filedb/test1/type_test", "type"->"file"))
    outputStore.append(zeroValue)
    outputStore.close()
    assert(zeroValue.readDouble(0, 0) == 1.2)
    assert(zeroValue.readLong(1, 0) == 22)
    assert(arr.readLong(0) == 0)
    val inputStore = FrameStoreAdapter(Map("path" -> "./tmp/unittests/RollFrameTests/filedb/test1/type_test", "type"->"file"))
    for(b <- inputStore.readAll()) {
      println(b.readDouble(0,0))
      println(b.readLong(1,0))
      println()
      (0 until 10).foreach(i => print(b.getValueVector(2, 0).readLong(i) + " "))
    }

  }

  @Test
  def testTmp():Unit = {

  }
}
