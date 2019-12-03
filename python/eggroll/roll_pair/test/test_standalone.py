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
import argparse
import time
import unittest
# from roll_pair_test_assets import *
from eggroll.core.session import ErSession
from eggroll.roll_pair.egg_pair import serve
from eggroll.roll_pair.roll_pair import RollPairContext
import os, sys

from multiprocessing import Process

import threading

class StandaloneManagerThread (threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        # self.setDaemon(True)

    def run(self):
        print ("StandaloneManagerThread start：" + self.name)
        os.system("./bin/eggroll_boot.sh start_node './bin/eggroll_boot_standalone_manager.sh -p 4670' s1 node1 ")
        print ("StandaloneManagerThread stop：" + self.name)

    def stop(self):
        os.system("./bin/eggroll_boot.sh stop_node './bin/eggroll_boot_standalone_manager.sh  -p 4670' s1 node1 ")

class EggPairThread (threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        # self.setDaemon(True)

    def run(self):
        print ("EggPairThread start：" + self.name)
        parser = argparse.ArgumentParser()
        parser.add_argument('-d', '--data-dir', default=os.path.dirname(os.path.realpath(__file__)))
        parser.add_argument('-n', '--node-manager')
        parser.add_argument('-s', '--session-id')
        parser.add_argument('-p', '--port', default='4672')

        args = parser.parse_args()
        serve(args)
        print ("EggPairThread stop：" + self.name)

    # def stop(self):
    #     os.system("./bin/eggroll_boot.sh stop_node './bin/eggroll_boot_standalone_manager.sh  -p 4670' s1 node1 ")

class TestStandalone(unittest.TestCase):
    def test_get(self):
        time.sleep(5)
        session = ErSession(options={"eggroll.deploy.mode": "standalone"})
        context = RollPairContext(session)
        context.load("ns1", "n22").put("k1", "v1")
        print(context.load("ns1", "n22").get("k1"))
        # th.stop()

    def test_tmp(self):
        th = StandaloneManagerThread()
        th.start()
        print("s done")
        time.sleep(4)
        th.stop()
        print("done")
