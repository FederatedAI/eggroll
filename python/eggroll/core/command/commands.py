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

from eggroll.core.command.command_model import CommandURI

DEFAULT_DELIM = '/'


def _to_service_name(prefix, method_name, delim=DEFAULT_DELIM):
    return f'{prefix}{delim}{method_name}'


class MetadataCommands(object):
    prefix = 'v1/cluster-manager/metadata'

    get_server_node = 'getServerNode'
    get_server_node_service_name = _to_service_name(prefix, get_server_node)
    GET_SERVER_NODE = CommandURI(get_server_node_service_name)

    get_server_nodes = 'getServerNodes'
    get_server_nodes_service_name = _to_service_name(prefix, get_server_nodes)
    GET_SERVER_NODES = CommandURI(get_server_nodes_service_name)

    get_or_create_server_node = 'getOrCreateServerNode'
    get_or_create_server_node_service_name = _to_service_name(prefix,
                                                              get_or_create_server_node)
    GET_OR_CREATE_SERVER_NODE = CommandURI(
        get_or_create_server_node_service_name)

    create_or_update_server_node = 'createOrUpdateServerNode'
    create_or_update_server_node_service_name = _to_service_name(prefix,
                                                                 create_or_update_server_node)
    CREATE_OR_UPDATE_SERVER_NODE = CommandURI(
        create_or_update_server_node_service_name)

    get_store = 'getStore'
    get_store_service_name = _to_service_name(prefix, get_store)
    GET_STORE = CommandURI(get_store_service_name)

    get_or_create_store = 'getOrCreateStore'
    get_or_create_store_service_name = _to_service_name(prefix,
                                                        get_or_create_store)
    GET_OR_CREATE_STORE = CommandURI(get_or_create_store_service_name)

    delete_store = 'deleteStore'
    delete_store_service_name = _to_service_name(prefix, delete_store)
    DELETE_STORE = CommandURI(delete_store_service_name)

    get_store_from_namespace = 'getStoreFromNamespace'
    get_store_from_namespace_service_name = _to_service_name(prefix,
                                                        get_store_from_namespace)
    GET_STORE_FROM_NAMESPACE = CommandURI(get_store_from_namespace_service_name)

class NodeManagerCommands(object):
    prefix = 'v1/node-manager/processor'

    get_or_create_processor_batch = 'getOrCreateProcessorBatch'
    get_or_create_processor_batch_service_name = _to_service_name(prefix,
                                                                  get_or_create_processor_batch)
    GET_OR_CREATE_PROCESSOR_BATCH = CommandURI(
        get_or_create_processor_batch_service_name)

    get_or_create_servicer = 'getOrCreateServicer'
    get_or_create_servicer_service_name = _to_service_name(prefix,
                                                           get_or_create_servicer)
    GET_OR_CREATE_SERVICER = CommandURI(get_or_create_servicer_service_name)

    heartbeat = 'heartbeat'
    heartbeat_service_name = _to_service_name(prefix, heartbeat)
    HEARTBEAT = CommandURI(heartbeat_service_name)


class SessionCommands(object):
    prefix = 'v1/cluster-manager/session'
    get_or_create_session = 'getOrCreateSession'
    get_or_create_session_service_name = _to_service_name(prefix,
                                                          get_or_create_session)
    GET_OR_CREATE_SESSION = CommandURI(get_or_create_session_service_name)

    register_session = 'registerSession'
    register_session_service_name = _to_service_name(prefix, register_session)
    REGISTER_SESSION = CommandURI(register_session_service_name)

    get_session_server_nodes = 'getSessionServerNodes'
    get_session_server_nodes_service_name = _to_service_name(prefix,
                                                             get_session_server_nodes)
    GET_SESSION_SERVER_NODES = CommandURI(get_session_server_nodes_service_name)

    get_session_rolls = "getSessionRolls"
    get_session_rolls_service_name = _to_service_name(prefix, get_session_rolls)
    GET_SESSION_ROLLS = CommandURI(get_session_rolls_service_name)

    get_session_eggs = "getSessionEggs"
    get_session_eggs_service_name = _to_service_name(prefix, get_session_eggs)
    GET_SESSION_EGGS = CommandURI(get_session_eggs_service_name)

    heartbeat = 'heartbeat'
    heartbeat_service_name = _to_service_name(prefix, heartbeat)
    HEARTBEAT = CommandURI(heartbeat_service_name)

    stop_session = 'stopSession'
    stop_session_service_name = _to_service_name(prefix, stop_session)
    STOP_SESSION = CommandURI(stop_session_service_name)

    kill_session = 'killSession'
    kill_session_service_name = _to_service_name(prefix, kill_session)
    KILL_SESSION = CommandURI(kill_session_service_name)

    kill_all_sessions = "killAllSessions"
    kill_all_sessions_service_name = _to_service_name(prefix, kill_all_sessions)
    KILL_ALL_SESSIONS = CommandURI(kill_all_sessions_service_name)


class RollPairCommands(object):
    roll_prefix = 'v1/roll-pair'
    egg_prefix = 'v1/egg-pair'
