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

import java.nio.charset.StandardCharsets

import javax.xml.bind.DatatypeConverter

object NetworkConstants {
  val DEFAULT_LOCALHOST_IP = "127.0.0.1"

  val TRANSFER_PROTOCOL_MAGIC_NUMBER : Array[Byte] = DatatypeConverter.parseHexBinary("46709394")
  val TRANSFER_PROTOCOL_VERSION : Array[Byte] = DatatypeConverter.parseHexBinary("00000001")
}
