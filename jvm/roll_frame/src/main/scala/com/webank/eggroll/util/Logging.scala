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
 */

package com.webank.eggroll.util

import org.slf4j.{Logger, LoggerFactory}

/**
 *  A way to print driver log, but is inconsistent with the egg's.
 */
trait Logging {

  protected lazy implicit val logger: Logger = LoggerFactory.getLogger(getClass)

  def trace(message: => String): Unit = {
    if (logger.isTraceEnabled) {
      logger.trace(message.toString)
    }
  }

  def trace(message: => String, t: Throwable): Unit = {
    logger.trace(message.toString, t)
  }

  def logTrace(msg: => String): Unit = trace(msg)

  def logTrace(msg: => String, t: Throwable): Unit = trace(msg, t)

  def debug(message: => String): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(message.toString)
    }
  }

  def debug(message: => String, t: Throwable): Unit = {
    logger.debug(message.toString, t)
  }

  def logDebug(msg: => String): Unit = debug(msg)

  def logDebug(msg: => String, t: Throwable): Unit = debug(msg, t)

  def info(message: => String): Unit = {
    if (logger.isInfoEnabled) {
      logger.info(message.toString)
    }
  }

  def info(message: => String, t: Throwable): Unit = {
    logger.info(message.toString, t)
  }

  def logInfo(msg: => String): Unit = info(msg)

  def logInfo(msg: => String, t: Throwable): Unit = info(msg, t)

  def warn(message: => String): Unit = {
    logger.warn(message.toString)
  }

  def warn(message: => String, t: Throwable): Unit = {
    logger.warn(message.toString, t)
  }

  def logWarn(msg: => String): Unit = warn(msg)

  def logWarn(msg: => String, t: Throwable): Unit = warn(msg, t)

  def logWarning(msg: => String): Unit = warn(msg)

  def logWarning(msg: => String, t: Throwable): Unit = warn(msg, t)

  def error(message: => String): Unit = {
    logger.error(message.toString)
  }

  def error(message: => String, t: Throwable): Unit = {
    logger.error(message.toString, t)
  }

  def logError(msg: => String): Unit = error(msg)

  def logError(msg: => String, t: Throwable): Unit = error(msg, t)
}