# -*- coding: utf-8 -*-
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


import time
import unittest
# from roll_pair_test_assets import *
from eggroll.core.session import ErSession
from eggroll.roll_pair.roll_pair import RollPairContext
from eggroll.core.conf_keys import DeployConfKeys, SessionConfKeys


class TestRollPairContext(unittest.TestCase):
    def test_init(self):
        # sess = ErSession(options={"eggroll.deploy.mode": "standalone"})
        options = {}
        base_dir = '/Users/huangqijun/codespace/eggroll_v2/jvm/roll_pair/'

        options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_VENV_PATH] = '/Users/huangqijun/anaconda3'
        options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_DATA_DIR_PATH] = '/tmp/eggroll'
        options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_EGGPAIR_PATH] = '/Users/huangqijun/codespace/eggroll_v2/python/eggroll/roll_pair/egg_pair.py'
        options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_PYTHON_PATH] = '/Users/huangqijun/codespace/eggroll_v2/python'
        options[DeployConfKeys.CONFKEY_DEPLOY_JVM_MAINCLASS] = 'com.webank.eggroll.rollpair.Main'
        options[DeployConfKeys.CONFKEY_DEPLOY_JVM_CLASSPATH] = f'{base_dir}/target/lib/*:{base_dir}/target/eggroll-roll-pair-2.0.jar:{base_dir}/resources'
        options[SessionConfKeys.CONFKEY_SESSION_ID] = 'testing'
        options[SessionConfKeys.CONFKEY_SESSION_MAX_PROCESSORS_PER_NODE] = '1'

        print(options)
        sess = ErSession(options = options)
        ctx = RollPairContext(sess)
        # ctx.load("ns1", "n21").put("k1", "v1")
        print(ctx.load("ns1", "n21").get("k1"))

