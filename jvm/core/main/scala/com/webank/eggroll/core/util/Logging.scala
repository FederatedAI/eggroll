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

import java.io.File
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean

import com.webank.eggroll.core.constant.StringConstants
import org.apache.commons.lang3.StringUtils
import org.apache.logging.log4j.core.LoggerContext
import org.apache.logging.log4j.core.config.Configurator
import org.apache.logging.log4j.{Level, LogManager, Logger}

trait Logging {
  @transient private var log_ : Logger = _
  private var prefix: String = StringConstants.EMPTY
  private val usingDefaultPropertiesAndNotWarned = new AtomicBoolean(false)

  protected def setLogPrefix(prefix: String): Unit = {
    if (StringUtils.isNotBlank(prefix)) {
      this.prefix = prefix
      if (!prefix.endsWith(StringConstants.SPACE)) {
        this.prefix += StringConstants.SPACE
      }
    }
  }

  protected def loggerName: String = {
    this.getClass.getName.stripSuffix(StringConstants.DOLLAR)
  }

  protected def log: Logger = {
    var defaultLogProperties: File = null
    if (log_ == null) {
      if (!Logging.logContextInited.get()) synchronized {
        var logContext = LogManager.getContext(false).asInstanceOf[LoggerContext]
        var logConf = logContext.getConfiguration

        if (logConf.getAppenders.size() <= 1) {
          val eggrollHome = System.getenv("EGGROLL_HOME")
          if (StringUtils.isBlank(eggrollHome)) throw new IllegalStateException("EGGROLL_HOME is not set")

          defaultLogProperties = new File(s"${eggrollHome}/conf/log4j2.properties")
          if (!Files.exists(defaultLogProperties.toPath)) {
            throw new IllegalStateException(s"log properties not found and default log path ${defaultLogProperties.getAbsolutePath} not found")
          }

          Configurator.initialize(null, defaultLogProperties.getAbsolutePath)
          logContext.reconfigure()
          logContext = LogManager.getContext(false).asInstanceOf[LoggerContext]
          logConf = logContext.getConfiguration

          usingDefaultPropertiesAndNotWarned.set(true)
        }

        val eggrollLogLevelString = System.getenv("EGGROLL_LOG_LEVEL")
        var eggrollLogLevel = if (StringUtils.isBlank(eggrollLogLevelString)) Level.INFO else Level.getLevel(eggrollLogLevelString)
        if (eggrollLogLevel == null) eggrollLogLevel = Level.INFO

        val eggrollLogConsoleString = System.getenv("EGGROLL_LOG_CONSOLE")
        val eggrollLogConsole: Boolean = if ("1".equals(eggrollLogConsoleString)) true
        else java.lang.Boolean.valueOf(eggrollLogConsoleString)

        if (Level.DEBUG.compareTo(eggrollLogLevel) <= 0 || eggrollLogConsole) {
          logConf.getRootLogger.addAppender(logConf.getAppender("STDOUT"), eggrollLogLevel, null)
        }
        Logging.logContextInited.compareAndSet(false, true)

        if (usingDefaultPropertiesAndNotWarned.get()) {
          LogManager.getLogger().warn(s"Log4j2 properties file not set. using default properties file at ${defaultLogProperties.getAbsolutePath}")
          usingDefaultPropertiesAndNotWarned.set(false)
        }
      }

      log_ = LogManager.getLogger(loggerName)
    }

    log_
  }

  protected def isLogTraceEnabled(): Boolean = {
    log.isTraceEnabled
  }

  protected def isLogDebugEnabled(): Boolean = {
    log.isDebugEnabled
  }

  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(s"${prefix}${msg}")
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(s"${prefix}${msg}")
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(s"${prefix}${msg}")
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(s"${prefix}${msg}")
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(s"${prefix}${msg}")
  }

  // Log methods that take Throwables (Exceptions/Errors) too
  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(s"${prefix}${msg}", throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(s"${prefix}${msg}", throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(s"${prefix}${msg}", throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(s"${prefix}${msg}", throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(s"${prefix}${msg}", throwable)
  }

  protected def logError(throwable: Throwable): Unit = {
    if (log.isErrorEnabled) log.error(s"${prefix}${StringConstants.LOGGING_A_THROWABLE}", throwable)
  }
}

object Logging {
  val logContextInited = new AtomicBoolean(false)
}
