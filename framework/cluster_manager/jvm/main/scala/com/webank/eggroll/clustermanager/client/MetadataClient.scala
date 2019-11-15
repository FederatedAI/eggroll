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

package com.webank.eggroll.clustermanager.client

import com.webank.eggroll.core.constant.ClusterManagerConfKeys
import com.webank.eggroll.core.factory.GrpcChannelFactory
import com.webank.eggroll.core.meta.ErServerNode
import com.webank.eggroll.core.session.DefaultErConf

class MetadataClient {
  private def init(): Unit = {
    val clusterManagerHost = DefaultErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST)
    val clusterMangerPort = DefaultErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT)

  }

/*  def getServerNode(input: ErServerNode): ErServerNode = {

  }*/
}
