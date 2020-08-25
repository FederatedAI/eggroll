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

package com.webank.eggroll.rollsite

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.webank.ai.eggroll.api.networking.proxy.Proxy
import io.grpc.stub.ServerCallStreamObserver


object PollingHelper {
  val pollingSOs: LoadingCache[String, LinkedBlockingQueue[ServerCallStreamObserver[Proxy.PollingFrame]]] = CacheBuilder.newBuilder
    .maximumSize(100000)
    // TODO:0: configurable
    .expireAfterAccess(1, TimeUnit.HOURS)
    .concurrencyLevel(50)
    .recordStats
    .build(new CacheLoader[String, LinkedBlockingQueue[ServerCallStreamObserver[Proxy.PollingFrame]]]() {
      override def load(key: String): LinkedBlockingQueue[ServerCallStreamObserver[Proxy.PollingFrame]] = {
        new LinkedBlockingQueue[ServerCallStreamObserver[Proxy.PollingFrame]]()
      }
    })
}
