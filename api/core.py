#
#  Copyright 2019 The Eggroll Authors. All Rights Reserved.
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
#

from typing import MutableMapping

from eggroll.api import NamingPolicy, ComputingEngine
from eggroll.api.proto.basic_meta_pb2 import SessionInfo

class EggrollSession(object):
    def __init__(self, session_id, computing_engine_conf : MutableMapping = None, naming_policy : NamingPolicy = NamingPolicy.DEFAULT, tag=None):
        if not computing_engine_conf:
            computing_engine_conf = dict()
        self._session_id = session_id
        self._computing_engine_conf = computing_engine_conf
        self._naming_policy = naming_policy
        self._tag = tag
        self._cleanup_tasks = set()
        self._runtime = dict()

    def get_session_id(self):
        return self._session_id

    def computing_engine_conf(self):
        return self.computing_engine_conf

    def add_conf(self, key, value):
        self._computing_engine_conf[key] = str(value)

    def get_conf(self, key):
        return self._computing_engine_conf.get(key)

    def has_conf(self, key):
        return self.get_conf(key) is not None

    def get_naming_policy(self):
        return self._naming_policy

    def get_tag(self):
        return self._tag

    def add_cleanup_task(self, func):
        self._cleanup_tasks.add(func)

    def run_cleanup_tasks(self):
        for func in self._cleanup_tasks:
            func()

    def to_protobuf(self):
        return SessionInfo(sessionId=self._session_id,
                                         computingEngineConf=self._computing_engine_conf,
                                         namingPolicy=self._naming_policy.name,
                                         tag=self._tag)

    @staticmethod
    def from_protobuf(session):
        return EggrollSession(session_id=session.get_session_id(),
                              computing_engine_conf=session.get_computing_engine_conf(),
                              naming_policy=session.get_naming_policy(),
                              tag=session.get_tag())

    def set_runtime(self, computing_engine : ComputingEngine, target):
        self._runtime[computing_engine] = target

    def __str__(self):
        return "<EggrollSession: session_id: {}, computing_engine_conf: {}, naming_policy: {}, tag: {}, runtime: {}>"\
            .format(self._session_id, self.computing_engine_conf(), self._naming_policy.name, self._tag, self._runtime)