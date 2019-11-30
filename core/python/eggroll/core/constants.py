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
  ROLLPAIR_LMDB = 'rollpair.lmdb'
  ROLLPAIR_LEVELDB = 'rollpair.leveldb'
  ROLLFRAME_FILE = 'rollframe.file'

class PartitionerTypes(object):
  BYTESTRING_HASH = 'BYTESTRING_HASH'

class ProcessorTypes(object):
  EGG_PAIR = 'egg_pair'
  ROLL_PAIR_SERVICER = 'roll_pair_servicer'

class RollTypes(object):
  ROLL_PAIR = 'roll_pair'

class ProcessorStatus(object):
  RUNNING = 'RUNNING'

class SessionStatus(object):
  NEW = 'NEW'
  RUNNING = 'RUNNING'
  TERMINATED = 'TERMINATED'