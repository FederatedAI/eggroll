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

package com.webank.eggroll.core.nodemanager

import java.util.Properties

import com.webank.eggroll.core.schedule.deploy.ExecutableProcessorOperator
import com.webank.eggroll.core.session.RuntimeErConf
import org.junit.Test

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

class TestOperator {
  private val testProps = new Properties()
  testProps.setProperty("eggroll.node.exe","ping 127.0.0.1")
  testProps.setProperty("eggroll.node.id","101")
  testProps.setProperty("eggroll.node.log.dir","./logs")
  testProps.setProperty("eggroll.session.id","s1")
  testProps.setProperty("eggroll.boot.script", "../../bin/eggroll_boot.sh")
  testProps.setProperty("exe","ls")
  @Test
  def testStartNode():Unit = {
    val op = new ExecutableProcessorOperator(RuntimeErConf(testProps))
    op.start()
//    op.stop()
  }

  @Test
  def testStopNode():Unit = {
    val op = new ExecutableProcessorOperator(RuntimeErConf(testProps))
    //    op.start()
    op.stop()
  }
}
