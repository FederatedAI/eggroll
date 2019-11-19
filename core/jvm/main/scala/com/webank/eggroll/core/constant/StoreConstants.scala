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
  private val delim = StringConstants.DOT
  val ROLLPAIR_LMDB = String.join(delim, StringConstants.ROLL_PAIR, StringConstants.LMDB)
  val ROLLPAIR_LEVELDB = String.join(delim, StringConstants.ROLL_PAIR, StringConstants.LEVELDB)
  val ROLLFRAME_FILE = String.join(delim, StringConstants.ROLL_FRAME, StringConstants.FILE)
}

object PartitionerTypes {
  val BYTESTRING_HASH = "BYTESTRING_HASH"
}

object SerdesTypes {
  val PICKLE = "PICKLE"
  val PROTOBUF = "PROTOBUF"
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