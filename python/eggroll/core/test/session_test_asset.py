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

from eggroll.core.conf_keys import SessionConfKeys, TransferConfKeys, \
    ClusterManagerConfKeys, NodeManagerConfKeys
from eggroll.core.constants import DeployModes
from eggroll.core.constants import ProcessorTypes, ProcessorStatus
from eggroll.core.constants import StoreTypes
from eggroll.core.meta_model import ErStore, ErStoreLocator, ErEndpoint, \
    ErProcessor
from eggroll.core.session import ErSession

ER_STORE1 = ErStore(
        store_locator=ErStoreLocator(store_type=StoreTypes.ROLLPAIR_LEVELDB,
                                     namespace="namespace",
                                     name="name"))


def get_debug_test_context(is_standalone=False, manager_port=4670, egg_port=20001, transfer_port=20002, session_id='testing'):
    manager_port = manager_port
    egg_ports = [egg_port]
    egg_transfer_ports = [transfer_port]
    self_server_node_id = 2

    options = {}
    if is_standalone:
        options[SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE] = "standalone"
    options[TransferConfKeys.CONFKEY_TRANSFER_SERVICE_HOST] = "127.0.0.1"
    options[TransferConfKeys.CONFKEY_TRANSFER_SERVICE_PORT] = str(transfer_port)
    options[ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT] = str(manager_port)
    options[NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT] = str(manager_port)

    egg = ErProcessor(id=1,
                      server_node_id=self_server_node_id,
                      processor_type=ProcessorTypes.EGG_PAIR,
                      status=ProcessorStatus.RUNNING,
                      command_endpoint=ErEndpoint("127.0.0.1", egg_ports[0]),
                      transfer_endpoint=ErEndpoint("127.0.0.1",
                                                   egg_transfer_ports[0]))

    roll = ErProcessor(id=1,
                       server_node_id=self_server_node_id,
                       processor_type=ProcessorTypes.ROLL_PAIR_MASTER,
                       status=ProcessorStatus.RUNNING,
                       command_endpoint=ErEndpoint("127.0.0.1", manager_port))

    session = ErSession(session_id,
                        processors=[egg, roll],
                        options=options)
    print(session.get_session_id())
    return session


def get_standalone_context():
    options = {
        SessionConfKeys.CONFKEY_SESSION_DEPLOY_MODE: DeployModes.STANDALONE}

    session = ErSession(options=options)
    print(session.get_session_id())
    return session


def get_cluster_context(options=None):
    if options is None:
        options = {}
    session = ErSession(options=options)
    print(session.get_session_id())
    return session


default_option = {}


def set_default_option(k, v):
    default_option[k] = v


def get_default_options():
    return default_option.copy()
