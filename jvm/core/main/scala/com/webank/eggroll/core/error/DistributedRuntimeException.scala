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

package com.webank.eggroll.core.error

import com.webank.eggroll.core.constant.StringConstants
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.mutable

class DistributedRuntimeException extends RuntimeException {
  private val causes = mutable.ListBuffer[Throwable]()

  def append(t: Throwable): Unit = synchronized(causes.append(t))

  def raise(): Unit = {
    if (causes.nonEmpty) {
      throw this
    }
  }

  def checkEmpty(): Boolean = {
    causes.isEmpty
  }

  override def toString: String = {
    val sb = new StringBuilder
    sb.append("total number of exception(s) occured: ")
      .append(causes.length)
      .append(StringConstants.LF)

    for ((cause, i) <- causes.view.zipWithIndex) {
      sb.append("idx: ")
        .append(i)
        .append(StringConstants.LF)
        .append(ExceptionUtils.getStackTrace(cause))
        .append(StringConstants.LFLF)
    }

    sb.toString
  }

  override def getMessage: String = toString
}
