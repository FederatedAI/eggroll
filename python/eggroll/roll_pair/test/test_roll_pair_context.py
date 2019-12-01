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
from eggroll.core.conf_keys import DeployConfKeys, SessionConfKeys, \
  ClusterManagerConfKeys, NodeManagerConfKeys


class TestRollPairContext(unittest.TestCase):
  def test_init(self):
    session = ErSession(options={"eggroll.deploy.mode": "standalone"})
    # session = ErSession()
    context = RollPairContext(session)
    #context.load("ns1", "n21").put("k1", "v1")
    print(context.load("ns1", "n21").get("k1"))


  def test_init_cluster(self):
    options = {}
    base_dir = '/Users/max-webank/git/eggroll-2.x'
    options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_VENV_PATH] = '/Users/max-webank/env/venv'
    options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_DATA_DIR_PATH] = '/tmp/eggroll'
    options[ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST] = 'localhost'
    options[ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT] = '4666'


    options[DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_PYTHON_PATH] = f'{base_dir}/python'
    options[
      DeployConfKeys.CONFKEY_DEPLOY_ROLLPAIR_EGGPAIR_PATH] = f'{base_dir}/python/eggroll/roll_pair/egg_pair.py'
    options[DeployConfKeys.CONFKEY_DEPLOY_JVM_MAINCLASS] = 'com.webank.eggroll.rollpair.Main'
    options[
      DeployConfKeys.CONFKEY_DEPLOY_JVM_CLASSPATH] = f'{base_dir}/jvm/roll_pair/target/lib/*:{base_dir}/jvm/roll_pair/target/eggroll-roll-pair-2.0.jar:{base_dir}/jvm/roll_pair/main/resources'
    options[SessionConfKeys.CONFKEY_SESSION_ID] = 'testing'
    options[SessionConfKeys.CONFKEY_SESSION_MAX_PROCESSORS_PER_NODE] = '1'

    session = ErSession(session_id='test_init', options=options)
    context = RollPairContext(session)

    context.load("ns1", "n21").put("k1", "v1")
    print(context.load("ns1", "n21").get("k1"))
