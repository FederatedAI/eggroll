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

import java.util

import org.junit.Test
import com.webank.eggroll.util.File
import jep.NDArray

class PyInterpreterTests {
  val interp: PyInterpreter = PyInterpreter()

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
        |print(m)
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
  def testTransferData(): Unit = {
    val f = Array[Double](1.1f, 2.2f, 3.3f, 4.4f, 5.5f, 6.6f)
    val nd = new NDArray[Array[Double]](f, 3, 2)

    interp.setValue("input", nd)
    interp.exec("import numpy as np\nimport torch\n" +
      "data = torch.from_numpy(input)\ndata = data * 2\noutput = data.numpy()")

    val input = interp.getValue("input").asInstanceOf[NDArray[_]]
    val output = interp.getValue("output").asInstanceOf[NDArray[_]]
    val inputData = input.getData.asInstanceOf[Array[Float]]
    val outputData = output.getData.asInstanceOf[Array[Float]]
    inputData.indices.foreach(i => assert(inputData(i) * 2 == outputData(i)))
  }

  @Test
  def testPyTorchLocal(): Unit = {
    // run lr model
    val pyCodes = File.getStringFromFile("jvm/roll_frame/src/test/resources/lr_test.py")
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
}
