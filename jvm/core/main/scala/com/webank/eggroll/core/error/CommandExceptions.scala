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

import com.webank.eggroll.core.command.CommandURI
import com.webank.eggroll.core.meta.ErEndpoint

class CommandRoutingException(serviceName: String, cause: Throwable)
  extends RuntimeException(s"[COMMANDROUTING] Failed to route to serviceName ${serviceName}", cause)

class CommandCallException(serviceName: String, endpoint: ErEndpoint, cause: Throwable)
  extends RuntimeException(s"[COMMANDCALL] Error while calling serviceName: ${serviceName} to endpoint: ${endpoint}", cause) {
  def this(commandUri: CommandURI, endpoint: ErEndpoint, cause: Throwable) = this(commandUri.uriString, endpoint, cause)
}