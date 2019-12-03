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
from eggroll.core.session import ErSession
from eggroll.roll_pair.roll_pair import RollPairContext


class TestStandalone(unittest.TestCase):
    def test_get(self):
        session = ErSession(options={"eggroll.deploy.mode": "standalone"})
        context = RollPairContext(session)
        context.load("ns1", "n22").put("k1", "v1")
        print(context.load("ns1", "n22").get("k1"))

