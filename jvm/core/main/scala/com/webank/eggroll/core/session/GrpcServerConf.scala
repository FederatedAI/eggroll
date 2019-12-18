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

package com.webank.eggroll.core.session

import java.util

import com.webank.eggroll.core.meta.ErEndpoint
import io.grpc.{BindableService, ServerServiceDefinition}

@deprecated
class GrpcServerConf(confFilePath: String) {
  var host: String = _
  var port: Int = -1
  var serverServiceDefinition: java.util.List[ServerServiceDefinition] = new util.ArrayList[ServerServiceDefinition]()
  var isSecureClient: Boolean = false
  var isSecureServer: Boolean = false

  def setHost(host: String): GrpcServerConf = {
    this.host = host
    this
  }

  def setPort(port: Int): GrpcServerConf = {
    this.port = port
    this
  }

  def addService(service: ServerServiceDefinition): GrpcServerConf = {
    serverServiceDefinition.add(service)
    this
  }

  def addService(service: BindableService): GrpcServerConf = {
    serverServiceDefinition.add(service.bindService())
    this
  }

  def endpoint(): ErEndpoint = {
    new ErEndpoint(host, port)
  }
}