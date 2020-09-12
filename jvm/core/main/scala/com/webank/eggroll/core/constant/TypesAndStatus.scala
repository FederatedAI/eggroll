/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.core.constant

object StoreTypes {
  val ROLLFRAME_FILE = "ROLL_FRAME_FILE"
  val ROLLPAIR_IN_MEMORY = "IN_MEMORY"
  val ROLLPAIR_LEVELDB = "LEVELDB"
  val ROLLPAIR_LMDB = "LMDB"
}

object PartitionerTypes {
  val BYTESTRING_HASH = "BYTESTRING_HASH"
}

object SerdesTypes {
  val PICKLE = "PICKLE"
  val PROTOBUF = "PROTOBUF"
  val CLOUD_PICKLE = "CLOUD_PICKLE"
  val EMPTY = "EMPTY"
}

object StoreStatus {
  val NORMAL = "NORMAL"
  val DELETED = "DELETED"
}

object PartitionStatus {
  val PRIMARY = "PRIMARY"
  val BACKUP = "BACKUP"
  val MISSING = "MISSING"
}

object TransferStatus {
  val TRANSFER_END = "__transfer_end"
}

object ProcessorTypes {
  val ROLL_PAIR = "roll_pair"
  val ROLL_PAILLIER_TENSOR = "roll_paillier_tensor"
  val EGG_FRAME = "egg_frame"
  val ROLL_FRAME = "roll_frame"

  val ROLL_PAIR_MASTER = "roll_pair_master"
  val EGG_PAIR = "egg_pair"
}

object ServerNodeTypes {
  val CLUSTER_MANAGER = "CLUSTER_MANAGER"
  val NODE_MANAGER = "NODE_MANAGER"
}

object ServerNodeStatus {
  val HEALTHY = "HEALTHY"
}

object ProcessorStatus {
  val NEW = "NEW"
  val RUNNING = "RUNNING"
  val STOPPED = "STOPPED"
  val KILLED = "KILLED"
  val ERROR = "ERROR"
}

object SessionStatus {
  val NEW = "NEW"
  val ACTIVE = "ACTIVE"
  val CLOSED = "CLOSED"
  val KILLED = "KILLED"
  val ERROR = "ERROR"
}

object BindingStrategies {
  val ROUND_ROBIN = "ROUND_ROBIN"
}