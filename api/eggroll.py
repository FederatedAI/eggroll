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

from eggroll.api.utils.log_utils import LoggerFactory
from eggroll.api.utils import file_utils
from typing import Iterable
import uuid
import os
from eggroll.api import WorkMode, NamingPolicy, ComputingEngine
from eggroll.api import RuntimeInstance
from eggroll.api.core import EggrollSession


def init(session_id=None, mode: WorkMode = WorkMode.STANDALONE, server_conf_path="eggroll/conf/server_conf.json", eggroll_session : EggrollSession = None, computing_engine_conf=None, naming_policy=NamingPolicy.DEFAULT, tag=None, job_id=None):
    if RuntimeInstance.EGGROLL:
        return
    if not session_id:
        session_id = str(uuid.uuid1())
    LoggerFactory.setDirectory(os.path.join(file_utils.get_project_base_directory(), 'logs', session_id))

    if not job_id:
        job_id = session_id
    RuntimeInstance.MODE = mode

    #eggroll_session = EggrollSession(naming_policy=naming_policy)
    if mode == WorkMode.STANDALONE:
        from eggroll.api.standalone.eggroll import Standalone
        RuntimeInstance.EGGROLL = Standalone(job_id=job_id, eggroll_session=eggroll_session)
    elif mode == WorkMode.CLUSTER:
        from eggroll.api.cluster.eggroll import _EggRoll
        from eggroll.api.cluster.eggroll import init as c_init
        c_init(session_id=session_id, server_conf_path=server_conf_path, computing_engine_conf=computing_engine_conf, naming_policy=naming_policy, tag=tag, job_id=job_id)
        RuntimeInstance.EGGROLL = _EggRoll.get_instance()
    else:
        from eggroll.api.cluster import simple_roll
        simple_roll.init(job_id)
        RuntimeInstance.EGGROLL = simple_roll.EggRoll.get_instance()
    RuntimeInstance.EGGROLL.table("__clustercomm__", job_id, partition=10)


def table(name, namespace, partition=1, persistent=True, create_if_missing=True, error_if_exist=False, in_place_computing=False):
    return RuntimeInstance.EGGROLL.table(name=name,
                                         namespace=namespace,
                                         partition=partition,
                                         persistent=persistent,
                                         in_place_computing=in_place_computing)


def parallelize(data: Iterable, include_key=False, name=None, partition=1, namespace=None, persistent=False,
                create_if_missing=True, error_if_exist=False, chunk_size=100000, in_place_computing=False):
    return RuntimeInstance.EGGROLL.parallelize(data=data, include_key=include_key, name=name, partition=partition,
                                               namespace=namespace,
                                               persistent=persistent,
                                               chunk_size=chunk_size,
                                               in_place_computing=in_place_computing)

def stop():
    RuntimeInstance.EGGROLL.stop()
    RuntimeInstance.EGGROLL = None

def get_eggroll_session():
    return RuntimeInstance.EGGROLL.get_eggroll_session()

def cleanup(name, namespace, persistent=False):
    return RuntimeInstance.EGGROLL.cleanup(name=name, namespace=namespace, persistent=persistent)

def generateUniqueId():
    return RuntimeInstance.EGGROLL.generateUniqueId()

def get_job_id():
    return RuntimeInstance.EGGROLL.job_id
