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


class SerdesTypes(object):
    PROTOBUF = 'PROTOBUF'
    PICKLE = 'PICKLE'
    CLOUD_PICKLE = 'CLOUD_PICKLE'
    EMPTY = 'EMPTY'


class ServerNodeStatus(object):
    HEALTHY = 'HEALTHY'


class ServerNodeTypes(object):
    CLUSTER_MANAGER = 'CLUSTER_MANAGER'
    NODE_MANAGER = 'NODE_MANAGER'


class StoreTypes(object):
    ROLLPAIR_IN_MEMORY = 'IN_MEMORY'
    ROLLPAIR_LMDB = 'LMDB'
    ROLLPAIR_LEVELDB = 'LEVELDB'
    ROLLFRAME_FILE = 'ROLL_FRAME_FILE'
    ROLLPAIR_ROLLSITE = 'ROLL_SITE'
    ROLLPAIR_FILE = 'ROLL_PAIR_FILE'
    ROLLPAIR_MMAP = 'ROLL_PAIR_MMAP'
    ROLLPAIR_CACHE = 'ROLL_PAIR_CACHE'
    ROLLPAIR_QUEUE = 'ROLL_PAIR_QUEUE'


class PartitionerTypes(object):
    BYTESTRING_HASH = 'BYTESTRING_HASH'


class ProcessorTypes(object):
    EGG_PAIR = 'egg_pair'
    ROLL_PAIR_MASTER = 'roll_pair_master'


class RollTypes(object):
    ROLL_PAIR = 'roll_pair'


class ProcessorStatus(object):
    NEW = 'NEW'
    RUNNING = 'RUNNING'
    STOPPED = 'STOPPED'
    KILLED = 'KILLED'


class SessionStatus(object):
    NEW = 'NEW'
    ACTIVE = 'ACTIVE'
    CLOSED = 'CLOSED'
    KILLED = 'KILLED'
    ERROR = 'ERROR'


class DeployModes(object):
    STANDALONE = 'standalone'
    CLUSTER = 'cluster'


class Charsets(object):
    UTF8 = 'utf8'
    ISO_8859_1 = 'iso-8859-1'