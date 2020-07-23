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
import org.junit.Test
import com.webank.eggroll.util.File

class PyInterpreterTests {
  val interp: PyInterpreter = PyInterpreter()

  @Test
  def demo(): Unit ={
    interp.close()
  }

  @Test
  def testRunPythonDemo(): Unit ={
    val codes =
      """
        |def max(a,b):
        |    return a if a>b else b
        |
        |a = 2
        |b = 3
        |m = max(a,b)
        |print(m)
        |""".stripMargin

    interp.exec(codes)
    val m = interp.getValue("m").asInstanceOf[Long]
    assert(m==3)
    interp.setValue("c",1)
    interp.setValue("d",2)
    val r = interp.invoke("max",Array(1),Array(2))

//    interp.invoke("max",1,2)
    val stop = 1
  }

  @Test
  def testPyTorch(): Unit ={
    // 先提供一个输入python字符的接口吧
    val pyCodes = File.getStringFromFile("jvm/roll_frame/src/test/resources/lr_test.py")
    // 假设数据已经有了，包含在代码中

    val stop = 1
  }
}
