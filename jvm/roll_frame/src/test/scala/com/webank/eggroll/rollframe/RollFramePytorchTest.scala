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

import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.ErStore
import com.webank.eggroll.format.{ColumnFrame, FrameBatch, FrameSchema, FrameStore, FrameVector}
import com.webank.eggroll.rollframe.pytorch.{LibraryLoader, Matrices, Torch}
import com.webank.eggroll.util.SchemaUtil
import junit.framework.TestCase
import org.junit.{Before, Test}

@deprecated
class RollFramePytorchTest {
  protected val ta: TestAssets.type = TestAssets
  val fieldCount: Int = 300
  protected val rowCount = 10000 // total value count = rowCount * fbCount * fieldCount
  protected var ctx: RollFrameContext = _
  protected var inputStore: ErStore = _
  protected val partitions_ = 2
  protected val supportTorch = true

  @Before
  def setup(): Unit = {
    ctx = ta.getRfContext()
    println(s"get RfContext property unsafe:${System.getProperty("arrow.enable_unsafe_memory_access")}")
    inputStore = ctx.createStore("test1", "ra1", StringConstants.CACHE, partitions_)
    if (supportTorch) {
        LibraryLoader.load
    }
    ctx.frameTransfer
  }

  def genData(): Unit = {
    (0 until partitions_).foreach { i =>
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(rowCount)), fieldCount)
      for {x <- 0 until rowCount
           y <- 0 until fieldCount} {
        fb.writeDouble(x, y, 1)
      }
      FrameStore(inputStore, i).append(fb)
    }
  }

  def genDataV1(): Unit = {
    (0 until partitions_).foreach { i =>
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fieldCount)), rowCount)
      for {x <- 0 until fieldCount
           y <- 0 until rowCount} {
        fb.writeDouble(x, y, 1)
      }
      FrameStore(inputStore, i).append(fb)
    }
  }

  def genTensorData(): Unit = {
    (0 until partitions_).foreach { i =>
      val fb = new FrameBatch(new FrameSchema(SchemaUtil.oneDoubleFieldSchema), fieldCount * rowCount)
      (0 until fieldCount * rowCount).foreach { i =>
        fb.writeDouble(0, i, 2)
      }
      FrameStore(inputStore, i).append(fb)
    }
  }


  /**
   * hessian map: Xt*X, slow
   */
  @Test
  def testHessianMatrix(): Unit = {
    // generate data
    var start = System.currentTimeMillis()
    genData()
    println(s"gen data time:${System.currentTimeMillis() - start} ms")
    val outStore = ctx.createStore("test1", "result1", StringConstants.CACHE, 1)
    val rf = ctx.load(inputStore)
    // zero init
    val fields = fieldCount
    // mock weight
    // val weights = Array.fill(fields)(1.0)
    val zero = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fields)), fields)
    zero.initZero()
    start = System.currentTimeMillis()
    rf.simpleAggregate(zero, (z, cb) => {
      // cal p ,dot mul
      val s = System.currentTimeMillis()
      cb.rootVectors.foreach { fv =>
        // val p = RollFrameAlgebraTest.sigmoid(weights, fv)
        (0 until z.fieldCount).foreach { f =>
          (0 until z.rowCount).foreach { r =>
//            val element = fv.readDouble(f) * fv.readDouble(r) * p * (1 - p)
            val element = fv.readDouble(f) * fv.readDouble(r)
            z.writeDouble(f, r, z.readDouble(f, r) + element)
          }
        }
      }
      println(s"${System.currentTimeMillis() - s}")
      z
    }, (x, y) => {
      (0 until x.fieldCount).foreach { f =>
        (0 until x.rowCount).foreach { r =>
          x.writeDouble(f, r, x.readDouble(f, r) + y.readDouble(f, r))
        }
      }
      x
    }, outStore)
    println(s"run time:${System.currentTimeMillis() - start} ms")
  }

  /**
   * hessian map: Xt*X, slow
   */
  @Test
  def testHessianMatrixV1(): Unit = {
    // generate data
    var start = System.currentTimeMillis()
    genDataV1()
    println(s"gen data time:${System.currentTimeMillis() - start} ms")
    val outStore = ctx.createStore("test1", "result1", StringConstants.CACHE, partitions_)
    val rf = ctx.load(inputStore)
    // zero init
    val fields = fieldCount
    // mock weight
    val zero = new FrameBatch(new FrameSchema(SchemaUtil.getDoubleSchema(fields)), fields)
    zero.initZero()
    start = System.currentTimeMillis()
    rf.simpleAggregate(zero, (z, cb) => {
      val s = System.currentTimeMillis()
      (0 until z.rowCount).foreach { i =>
        (0 until z.fieldCount).foreach { j =>
          var element = 0.0
          (0 until cb.rowCount).foreach { r =>
            element += cb.readDouble(i, r) * cb.readDouble(j, r)
          }
          z.writeDouble(i, j, element)
        }
      }
      println(s"${System.currentTimeMillis() - s}")
      z
    }, (x, y) => {
      (0 until x.fieldCount).foreach { f =>
        (0 until x.rowCount).foreach { r =>
          x.writeDouble(f, r, x.readDouble(f, r) + y.readDouble(f, r))
        }
      }
      x
    }, outStore)
    println(s"run time:${System.currentTimeMillis() - start} ms")
  }

  /**
   * hessian map: Xt*X, fast
   */
  @Test
  def testHessianMap(): Unit = {
    genTensorData()
    val output = ctx.forkStore(inputStore, "test1", "hessian_map", StringConstants.CACHE)
    val rf = ctx.load(inputStore)
    // Mock model parameter
    val v = Array.fill[Double](fieldCount)(1)
    val parameters: Array[Double] = Array(fieldCount.toDouble) ++ v

    val start = System.currentTimeMillis()
    rf.torchMap("jvm/roll_frame/src/test/resources/hessian_map.pt", parameters, output)
    println(s"time:${System.currentTimeMillis() - start} ms")
  }

  @Test
  def testHessianMerge(): Unit = {
    val output = ctx.createStore("test1", "a1TorchMerge", StringConstants.CACHE, totalPartitions = 1)
    val input = ctx.dumpCache(ctx.createStore("test1","hessian_map",StringConstants.FILE,totalPartitions = partitions_))
    val rf = ctx.load(input)
    val start = System.currentTimeMillis()
    rf.torchMerge(path = "jvm/roll_frame/src/test/resources/hessian_merge.pt", output = output)
    println(s"time:${System.currentTimeMillis() - start} ms")
    val path = FrameStore.getStorePath(output.partitions(0))
    val result = ctx.frameTransfer.Roll.pull(path).next()
    println(s"sum = ${result.readDouble(0, 0)},size = ${result.rowCount}")
  }

  @Test
  def testMergeByReduce(): Unit ={
    val output = ctx.createStore("test1", "a1TorchMerge", StringConstants.CACHE, totalPartitions = 1)
    val input = ctx.dumpCache(ctx.createStore("test1","hessian_map",StringConstants.FILE,totalPartitions = partitions_))
    val rf = ctx.load(input)
    val start = System.currentTimeMillis()
    rf.reduce((a,b)=>{
      (0 until a.fieldCount).foreach{f =>
        (0 until a.rowCount).foreach{ r =>
          a.writeDouble(f,r,a.readDouble(f,r)+b.readDouble(f,r))
        }
      }
      a
    },output)
    println(s"time:${System.currentTimeMillis() - start} ms")
    val path = FrameStore.getStorePath(output.partitions(0))
    val result = ctx.frameTransfer.Roll.pull(path).next()
    println(s"sum = ${result.readDouble(0, 0)},size = ${result.rowCount}")
  }


  @Test
  def testTorchScriptMap(): Unit = {
    genTensorData()
    val output = ctx.forkStore(inputStore, "test1", "a1TorchMap", StringConstants.FILE)
    val rf = ctx.load(inputStore)
    val newMatrixCols = 2
    val parameters: Array[Double] = Array(fieldCount.toDouble) ++ Array(newMatrixCols.toDouble) ++ Array.fill[Double](fieldCount * newMatrixCols)(0.5);

    val start = System.currentTimeMillis()
    rf.torchMap("jvm/roll_frame/src/test/resources/torch_model_map.pt", parameters, output)
    println(s"time:${System.currentTimeMillis() - start} ms")
    TestCase.assertEquals(FrameStore(output, 0).readOne().rowCount, rowCount * newMatrixCols)
  }

  @Test
  def testTorchScriptMerge(): Unit = {
    val input = ctx.dumpCache(ctx.forkStore(inputStore, "test1", "a1TorchMap", StringConstants.FILE))
    val output = ctx.createStore("test1", "a1TorchMerge", StringConstants.CACHE, totalPartitions = 1)
    val rf = ctx.load(input)
    val start = System.currentTimeMillis()
    rf.torchMerge(path = "jvm/roll_frame/src/test/resources/torch_model_merge.pt", output = output)
    println(s"time:${System.currentTimeMillis() - start} ms")
    val path = FrameStore.getStorePath(output.partitions(0))
    val result = ctx.frameTransfer.Roll.pull(path).next()
    println(s"sum = ${result.readDouble(0, 0)},size = ${result.rowCount}")
  }

  @Test
  @deprecated
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
}

object RollFramePytorchTest {
  def sigmoid(weights: Array[Double], features: FrameVector): Double = {
    var z = 0.0
    weights.indices.foreach { i =>
      z += weights(i) * features.readDouble(i)
    }
    val p = 1 / (1 + math.exp(0 - z))
    p
  }
}
