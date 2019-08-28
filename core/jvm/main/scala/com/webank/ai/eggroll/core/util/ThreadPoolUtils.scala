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

package com.webank.ai.eggroll.core.util

import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}

import scala.concurrent.ExecutionContext

object ThreadPoolUtils {
  private val directExecutionContext =
    ExecutionContext.fromExecutor(MoreExecutors.directExecutor())
  private lazy val defaultCachedNamedThreadPool =
    Executors.newCachedThreadPool(namedThreadFactory("default-cached"))

  def namedThreadFactory(prefix: String, isDaemon: Boolean = false): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(isDaemon).setNameFormat(prefix + "-%d").build()
  }

  def newFixedThreadPool(nThreads: Int, prefix: String, isDaemon: Boolean = false): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix, isDaemon)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def defaultThreadPool: ThreadPoolExecutor = defaultCachedNamedThreadPool.asInstanceOf[ThreadPoolExecutor]
}
