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
import com.webank.eggroll.format.{FrameBatch, FrameReader, FrameSchema, FrameWriter}
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
    val eggFrame = new EggFrame
    val opts = Map("path" -> path2)
    val cr = new FrameReader(path)
//    for( cb <- cr.getColumnarBatches){
//      eggFrame.write(cb, opts)
//    }
    val adapter = BlockStoreAdapter.create(opts)
    eggFrame.write(cr.getColumnarBatches, adapter)
    cr.close()
    adapter.close()
    val adapter2 = BlockStoreAdapter.create(opts)
    for( cb <- eggFrame.read(adapter2)){
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
  @Test
  def testColumnarWrite1000():Unit = {
    val fieldCount = 1000
    val schema = getSchema(fieldCount)
    val path = "./tmp/unittests/RollFrameTests/filedb/test1/a1/0"
    val cw = new FrameWriter(schema, path)
    val valueCount = 100*1000 / 2
    val batchSize = 100*1000 / 2
    cw.write(valueCount, batchSize,
      (fid, cv) => (0 until batchSize).foreach(
//        n => cv.writeDouble(n, fid * valueCount + n * 0.5)
        n => cv.writeDouble(n, fid  * new Random().nextDouble())
      )
    )
    cw.close()

  }

  @Test
  def testRollFrameAggregate(): Unit = {
    val sb = ServerBuilder.forPort(20101)
    sb.addService(new GrpcCommandService())
    sb.addService(new GrpcTransferService).build.start
    val sb2 = ServerBuilder.forPort(20100)
    sb2.addService(new GrpcCommandService())
    sb2.addService(new GrpcTransferService()).build.start
    val clusterManager = new ClusterManager
    val rf = new RollFrameService(clusterManager.getRollFrameStore("a1","test1"))
    val fieldCount = 1000
    val schema = getSchema(fieldCount)
    val zeroValue = new FrameBatch(new FrameSchema(schema), 1)
    rf.aggregate(zeroValue,{(x, y) =>
      try{
        for(f <- 0 until y.columnarVectors.size) {
          var sum = 0.0
          val fv = y.columnarVectors(f)
          for(i <- 0 until fv.valueCount) {
            sum += fv.readDouble(i)
          }
//          x.writeDouble(f,0, sum)
          x.writeDouble(f,0, fv.valueCount)
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
  }

  @Test
  def testTmp():Unit = {
//    val a = new Factory
//    val b:Foo = a.create()
//    println(b)
  }
}
