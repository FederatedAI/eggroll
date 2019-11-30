#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


from eggroll.core.session import ErSession
from eggroll.core.meta_model import ErSessionMeta
from eggroll.core.session import RollPairContext
from eggroll.core.conf_keys import DeployConfKeys, SessionConfKeys

import unittest

class TestSession(unittest.TestCase):
  def test_create_session(self):
    options = {}

    options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_VENV_PATH] = '/Users/max-webank/env/venv'
    options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_DATA_DIR_PATH] = '/tmp/eggroll'
    options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_EGGPAIR_PATH] = '/Users/max-webank/git/eggroll/roll_pair/egg_pair.py'
    options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_PYTHON_PATH] = '/Users/max-webank/git'
    options[DeployConfKeys.CONFKEY_DEPLOY_JVM_MAINCLASS] = 'com.webank.eggroll.rollpair.component.Main'
    options[DeployConfKeys.CONFKEY_DEPLOY_JVM_CLASSPATH] = '/Users/max-webank/git/eggroll-2.x/roll_objects/roll_pair/jvm/target/lib/*:/Users/max-webank/git/eggroll-2.x/roll_objects/roll_pair/jvm/target/eggroll-roll-pair-2.0.jar:/Users/max-webank/git/eggroll-2.x/framework/node_manager/jvm/test/resources'
    options[SessionConfKeys.CONFKEY_SESSION_ID] = 'test'
    options[SessionConfKeys.CONFKEY_SESSION_MAX_PROCESSORS_PER_NODE] = '100'

    print(options)
    session = ErSession(options = options)



if __name__ == '__main__':
  unittest.main()
