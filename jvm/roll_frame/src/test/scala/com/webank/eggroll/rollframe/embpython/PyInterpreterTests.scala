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

package com.webank.eggroll.rollframe.embpython

import java.nio.{ByteOrder, DoubleBuffer}
import java.util

import com.webank.eggroll.format.{FrameBatch, FrameSchema}
import org.junit.Test
import com.webank.eggroll.util.{File, SchemaUtil}
import jep.{DirectNDArray, NDArray}

class PyInterpreterTests {
  val interp: PyInterpreter = LocalThreadPythonInterp.interpreterThreadLocal.get

  @Test
  def testTransferParameters(): Unit = {
    // parameters support primary type and HashMap => PyJMap,ArrayList => PyJList
    val codes =
      """
        |def max_op(list):
        |    return max(list[0],list[1])
        |
        |a = [1,2]
        |m = max_op(a)
        |""".stripMargin

    interp.exec(codes)
    val m = interp.getValue("m").asInstanceOf[Long]
    assert(m == 2)

    val b = new util.ArrayList[Any]()
    b.add(3.0)
    b.add(4.4)
    interp.setValue("b", b)
    interp.exec("m = max_op(b)")
    val n = interp.getValue("m").asInstanceOf[Double]
    assert(n == 4.4)
  }

  @Test
  def testDirectNDArray(): Unit = {
    val fieldCount = 3
    val rowCount = 4
    val count = fieldCount * rowCount
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.oneDoubleFieldSchema), count)
    for (i <- 0 until count) {
      fb.writeDouble(0, i, i)
    }
    val data = fb.rootVectors(0).fieldVector.getDataBuffer.nioBuffer()
    data.order(ByteOrder.LITTLE_ENDIAN)
    val dnd = new DirectNDArray[DoubleBuffer](data.asDoubleBuffer(), count)
    interp.setValue("dnd", dnd)
    interp.exec("dnd[1] = 20")
    val res = interp.getValue("dnd").asInstanceOf[DirectNDArray[DoubleBuffer]]
    assert(fb.readDouble(0, 1) == res.getData.get(1))
  }

  @Test
  def testTransferData(): Unit = {
    val f = Array[Double](1.1, 2.2, 3.3, 4.4, 5.5, 6.6)
    val nd = new NDArray[Array[Double]](f, 3, 2)

    interp.setValue("input", nd)
    interp.exec("import numpy as np\nimport torch\n" +
      "data = torch.from_numpy(input)\ndata = data * 2\noutput = data.numpy()")

    val input = interp.getValue("input").asInstanceOf[NDArray[_]]
    val output = interp.getValue("output").asInstanceOf[NDArray[_]]
    val inputData = input.getData.asInstanceOf[Array[Double]]
    val outputData = output.getData.asInstanceOf[Array[Double]]
    inputData.indices.foreach(i => assert(inputData(i) * 2 == outputData(i)))
  }

  @Test
  def testPyTorchLocal(): Unit = {
    // run lr model
    val pyCodes = File.getStringFromFile(System.getProperty("user.dir")+"/jvm/roll_frame/src/test/resources/lr_test.py")
    // data generate in codes
    interp.exec(pyCodes)
    val w0 = interp.getValue("w0").asInstanceOf[NDArray[_]]
    val w1 = interp.getValue("w1").asInstanceOf[NDArray[_]]
    val d0 = w0.getDimensions
    val d1 = w1.getDimensions
    assert(d0(0) == 1)
    assert(d0(1) == 100)
    assert(d1(0) == 1)
  }

  @Test
  def testDirectBufferDiffThread(): Unit = {
    // main thread
    println(s"Thread: ${Thread.currentThread().getName}")
    val fieldCount = 3
    val rowCount = 4
    val count = fieldCount * rowCount
    val fb = new FrameBatch(new FrameSchema(SchemaUtil.oneDoubleFieldSchema), count)
    for (i <- 0 until count) {
      fb.writeDouble(0, i, i)
    }
    val data = fb.rootVectors(0).fieldVector.getDataBuffer.nioBuffer()
    data.order(ByteOrder.LITTLE_ENDIAN)
    val dnd = new DirectNDArray[DoubleBuffer](data.asDoubleBuffer(), count)
    interp.setValue("dnd", dnd)
    interp.exec("dnd[0] = 10")
    println(fb.rootVectors(0).getDataBufferAddress)
    // second thread
    val t = new Thread() {
      override def run(): Unit = {
        try {
          println(s"Thread: ${Thread.currentThread().getName}")
          val interp: PyInterpreter = LocalThreadPythonInterp.interpreterThreadLocal.get()
          val data1 = fb.rootVectors(0).fieldVector.getDataBuffer.nioBuffer()
          data1.order(ByteOrder.LITTLE_ENDIAN)
          val dnd1 = new DirectNDArray[DoubleBuffer](data1.asDoubleBuffer(), count)
          interp.setValue("dnd1", dnd1)
          interp.exec("dnd1[1] = 20")
        } catch {
          case e: Throwable => e.printStackTrace()
        }
      }
    }
    t.start()
    t.join()
    assert(fb.readDouble(0, 0) == 10.0)
    assert(fb.readDouble(0, 1) == 20.0)
  }
}
