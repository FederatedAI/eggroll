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
  val ROLLPAIR_LMDB = "rollpair.lmdb"
  val ROLLPAIR_LEVELDB = "rollpair.leveldb"
  val ROLLFRAME_FILE = "rollframe.file"
}

object PartitionerTypes {
  val BYTESTRING_HASH = "BYTESTRING_HASH"
}

object SerdesTypes {
  val PICKLE = "PICKLE"
  val PROTOBUF = "PROTOBUF"
  val CLOUD_PICKLE = "CLOUD_PICKLE"
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
  val ROLL_FRAME = "roll_frame"

  val ROLL_PAIR_SERVICER = "roll_pair_servicer"
  val EGG_PAIR = "egg_pair"
}

object NodeTypes {
  val CLUSTER_MANAGER = "CLUSTER_MANAGER"
  val NODE_MANAGER = "NODE_MANAGER"
}

object ServerNodeStatus {
  val HEALTHY = "HEALTHY"
}

object ProcessorStatus {
  val RUNNING = "RUNNING"
  val TERMINATED = "TERMINATED"
  val ERROR = "ERROR"
}

object SessionStatus {
  val NEW = "NEW"
  val ACTIVE = "ACTIVE"
  val CLOSED = "CLOSED"
}
