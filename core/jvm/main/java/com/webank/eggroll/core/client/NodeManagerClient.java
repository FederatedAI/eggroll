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

package com.webank.eggroll.core.client;

import com.webank.eggroll.core.command.CommandClient;
import com.webank.eggroll.core.command.CommandURI;
import com.webank.eggroll.core.constant.NodeManagerCommands;
import com.webank.eggroll.core.constant.NodeManagerConfKeys;
import com.webank.eggroll.core.constant.SerdesTypes;
import com.webank.eggroll.core.datastructure.RpcMessage;
import com.webank.eggroll.core.meta.ErEndpoint;
import com.webank.eggroll.core.meta.ErProcessor;
import com.webank.eggroll.core.meta.ErProcessorBatch;
import com.webank.eggroll.core.meta.ErSessionMeta;
import com.webank.eggroll.core.session.StaticErConf;

public class NodeManagerClient {
  private ErEndpoint nodeManagerEndpoint;
  private CommandClient commandClient;

  public NodeManagerClient(String serverHost, int serverPort) {
    this(new ErEndpoint(serverHost, serverPort));
  }

  public NodeManagerClient() {
    this(StaticErConf.getString(
        NodeManagerConfKeys.CONFKEY_NODE_MANAGER_HOST(), "localhost"),
        StaticErConf.getInt(
            NodeManagerConfKeys.CONFKEY_NODE_MANAGER_PORT(), 9394));
  }

  public NodeManagerClient(ErEndpoint serverEndpoint) {
    this.nodeManagerEndpoint = serverEndpoint;
    this.commandClient = new CommandClient();
  }

  public ErProcessorBatch getOrCreateProcessorBatch(ErSessionMeta sessionMeta) {
    return doSyncRequestInternal(sessionMeta, ErProcessorBatch.class, NodeManagerCommands.GET_OR_CREATE_PROCESSOR_BATCH());
  }

  public ErProcessorBatch getOrCreateServicer(ErSessionMeta sessionMeta) {
    return doSyncRequestInternal(sessionMeta, ErProcessorBatch.class, NodeManagerCommands.GET_OR_CREATE_SERVICER());
  }

  public ErProcessor heartbeat(ErProcessor processor) {
    return doSyncRequestInternal(processor, ErProcessor.class, NodeManagerCommands.HEARTBEAT());
  }

  private <T> T doSyncRequestInternal(RpcMessage input, Class<T> outputType, CommandURI commandURI) {
    return commandClient.simpleSyncSend(input, outputType, nodeManagerEndpoint, commandURI, SerdesTypes.PROTOBUF());
  }
}
