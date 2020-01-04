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

package com.webank.eggroll.core.util

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.commons.lang3.StringUtils

object TimeUtils {
  val noSeparatorFormatter = DateTimeFormatter.ofPattern("yyyyMMdd.HHmmss.SSS")

  def getNowMs(dateFormat: String = null): String = {
    val now = LocalDateTime.now()
    if (StringUtils.isBlank(dateFormat)) {
      noSeparatorFormatter.format(now)
    } else {
      new SimpleDateFormat(dateFormat).format(now)
    }
  }
}
