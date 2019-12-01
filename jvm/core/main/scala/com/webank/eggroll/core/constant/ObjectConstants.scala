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
 */

package com.webank.eggroll.core.constant

import scala.collection.mutable
import scala.collection.immutable


object ObjectConstants {
  val EMPTY_ARRAY_OF_BYTE_ARRAY = Array.empty[Array[Byte]]
  val EMPTY_MUTABLE_MAP_OF_STRING_TO_BYTE_ARRAY = mutable.Map.empty[String, Array[Byte]]
  val EMPTY_IMMUTABLE_MAP_OF_STRING_TO_BYTE_ARRAY = immutable.Map.empty[String, Array[Byte]]
  val EMPTY_PREDEF_MAP_OF_STRING_TO_BYTE_ARRAY = Map.empty[String, Array[Byte]]
  val EMPTY_LIST_OF_BYTE_ARRAY = List[Array[Byte]]()
}
