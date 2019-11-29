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

package com.webank.eggroll.clustermanager.session

import java.util.concurrent.ConcurrentHashMap

import com.webank.eggroll.core.meta.{ErSessionMeta, ErSession}

object SessionManager {
  private val activeSessions = new ConcurrentHashMap[String, ErSession]()


  def getOrCreateSession(session: ErSession): ErSession = this.synchronized {
    if (activeSessions.containsKey(session.id)) {
      activeSessions.get(session.id)
    } else {
      activeSessions.put(session.id, session)
    }
  }

  def getSession(sessionId: String): ErSession = this.synchronized {
    activeSessions.getOrDefault(sessionId, null)
  }

  def stopSession(session: ErSessionMeta): ErSession = this.synchronized {
    val sessionId = session.id
    if (activeSessions.containsKey(sessionId)) {
      activeSessions.remove(sessionId)
    }

    null
  }
}
