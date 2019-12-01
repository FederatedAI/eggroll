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

package com.webank.eggroll.core.schedule

import com.webank.eggroll.core.constant.{DeployConfKeys, SessionConfKeys}
import com.webank.eggroll.core.schedule.deploy.{JvmProcessorOperator, PythonProcessorOperator}
import com.webank.eggroll.core.session.RuntimeErConf
import org.junit.{Assert, Before, Test}

class TestProcessOperator {
  val conf = RuntimeErConf()
  var pythonOperator: PythonProcessorOperator = _
  var jvmOperator: JvmProcessorOperator = _
  @Before
  def preparePythonEnv(): Unit = {
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_VENV_PATH, "/Users/max-webank/env/venv")
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_DATA_DIR_PATH, "/tmp/eggroll")
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_EGGPAIR_PATH, "/Users/max-webank/git/eggroll/roll_pair/egg_pair.py")
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_PYTHON_PATH, "/Users/max-webank/git")
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_JVM_MAINCLASS, "com.webank.eggroll.rollpair.component.Main")
    conf.addProperty(DeployConfKeys.CONFKEY_DEPLOY_JVM_CLASSPATH, "/Users/max-webank/git/eggroll-2.x/roll_objects/roll_pair/jvm/target/lib/*:/Users/max-webank/git/eggroll-2.x/roll_objects/roll_pair/jvm/target/eggroll-roll-pair-2.0.jar")
    conf.addProperty(SessionConfKeys.CONFKEY_SESSION_ID, "test")

    pythonOperator = new PythonProcessorOperator()
    jvmOperator = new JvmProcessorOperator()
  }

  @Test
  def testStartPythonEggPair(): Unit = {
    val result = pythonOperator.start(conf)
    Assert.assertEquals(result, true)
  }

  @Test
  def testStopPythonEggPair(): Unit = {
    val result = pythonOperator.stop(20001)
    Assert.assertEquals(result, true)
  }

  @Test
  def testStartJvmRollPair(): Unit = {
    val result = jvmOperator.start(conf)
    Assert.assertEquals(result, true)
  }
}
