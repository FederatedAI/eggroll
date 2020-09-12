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

object StringConstants {
  val UNKNOWN = "UNKNOWN"

  val PROTOBUF = "PROTOBUF"

  val SLASH = "/"

  val GRPC_PARSE_FROM = "parseFrom"

  val AT = "@"
  val AND = "&"
  val EQUAL = "="

  val DOLLAR = "$"

  val DOT = "."
  val COLON = ":"
  val SEMICOLON = ";"
  val DASH = "-"
  val UNDERLINE = "_"
  val DOUBLE_UNDERLINES = "__"
  val COMMA = ","
  val HASH = "#"

  val HOST = "host"
  val PORT = "port"

  val META = "meta"
  val SEND_START = "send_start"
  val SEND_END = "send_end"

  val DEFAULT = "default"

  val ROLE_EGG = "egg"
  val ROLE_ROLL = "roll"
  val ROLE_EGGROLL = "eggroll"
  val EGGROLL_COMPATIBLE_ENABLED = "eggroll.compatible.enabled"
  val FALSE = "false"
  val TRUE = "true"

  val CLUSTER_COMM = "__clustercomm__"
  val FEDERATION = "__federation__"
  val EGGROLL = "eggroll"

  val COMPUTING = "computing"
  val STORAGE = "storage"

  val EMPTY = ""
  val SPACE = " "

  val LOGGING_A_THROWABLE = "logging a Throwable"

  val ROUTE = "route"

  val NULL = "null"
  val NULL_WITH_BRACKETS = s"[${NULL}]"

  val LF = "\n"
  val LFLF = "\n\n"



  val PATH = "path"
  val TYPE = "type"
  val SIZE = "size"

  val ROLL_PAIR = "rollpair"
  val ROLL_FRAME = "rollframe"
  val LMDB = "lmdb"
  val LEVELDB = "leveldb"
  val FILE = "file"
  val HDFS = "hdfs"
  val NETWORK = "network"
  val CACHE = "cache"
  val QUEUE = "queue"
  val TOTAL = "total"

  val LOCALHOST = "localhost"

  val STORE_TYPE = "storeType"
  val STORE_TYPE_SNAKECASE = "store_type"
  val NAMESPACE = "namespace"
  val NAME = "name"
  val TOTAL_PARTITIONS = "totalPartitions"
  val TOTAL_PARTITIONS_SNAKECASE = "total_partitions"
  val PARTITION_ID_SNAKECASE = "partition_id"
  val PARTITIONER = "partitioner"
  val SERDES = "serdes"
  val TRANSFER_BROKER_NAME = "transfer_broker_name"
}
