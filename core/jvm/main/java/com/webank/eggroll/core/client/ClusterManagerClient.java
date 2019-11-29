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
import com.webank.eggroll.core.constant.ClusterManagerConfKeys;
import com.webank.eggroll.core.constant.MetadataCommands;
import com.webank.eggroll.core.constant.SerdesTypes;
import com.webank.eggroll.core.constant.SessionCommands;
import com.webank.eggroll.core.datastructure.RpcMessage;
import com.webank.eggroll.core.meta.ErEndpoint;
import com.webank.eggroll.core.meta.ErPartition;
import com.webank.eggroll.core.meta.ErProcessorBatch;
import com.webank.eggroll.core.meta.ErServerCluster;
import com.webank.eggroll.core.meta.ErServerNode;
import com.webank.eggroll.core.meta.ErSessionMeta;
import com.webank.eggroll.core.meta.ErStore;
import com.webank.eggroll.core.meta.ErStoreLocator;
import com.webank.eggroll.core.session.StaticErConf;

public class ClusterManagerClient {
  private ErEndpoint clusterManagerEndpoint;
  private CommandClient commandClient;
  private static final ErPartition[] EMPTY_PARTITION_ARRAY = new ErPartition[0];

  public ClusterManagerClient(String serverHost, int serverPort) {
    this(new ErEndpoint(serverHost, serverPort));
  }

  public ClusterManagerClient() {
    this(StaticErConf.getString(
            ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_HOST(), "localhost"),
        StaticErConf.getInt(
            ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT(), 4670));
  }

  public ClusterManagerClient(ErEndpoint serverEndpoint) {
    this.clusterManagerEndpoint = serverEndpoint;
    this.commandClient = new CommandClient();
  }

  public ErServerNode getServerNode(ErServerNode input) {
    return doSyncRequestInternal(input, ErServerNode.class, MetadataCommands.GET_SERVER_NODE());
  }

  public ErServerCluster getServerNodes(ErServerNode input) {
    return doSyncRequestInternal(input, ErServerCluster.class, MetadataCommands.GET_SERVER_NODES());
  }

  public ErServerNode getOrCreateServerNode(ErServerNode input) {
    return doSyncRequestInternal(input, ErServerNode.class, MetadataCommands.GET_OR_CREATE_SERVER_NODE());
  }

  public ErServerNode createOrUpdateServerNode(ErServerNode input) {
    return doSyncRequestInternal(input, ErServerNode.class, MetadataCommands.CREATE_OR_UPDATE_SERVER_NODE());
  }

  public ErStore getStore(ErStoreLocator input) {
    return getStore(new ErStore(input, EMPTY_PARTITION_ARRAY));
  }

  public ErStore getStore(ErStore input) {
    return doSyncRequestInternal(input, ErStore.class, MetadataCommands.GET_STORE());
  }

  public ErStore getOrCreateStore(ErStoreLocator input) {
    return getOrCreateStore(new ErStore(input, EMPTY_PARTITION_ARRAY));
  }

  public ErStore getOrCreateStore(ErStore input) {
    return doSyncRequestInternal(input, ErStore.class, MetadataCommands.GET_OR_CREATE_STORE());
  }

  public ErStore deleteStore(ErStore input) {
    return doSyncRequestInternal(input, ErStore.class, MetadataCommands.DELETE_STORE());
  }

  public ErStore deleteStore(ErStoreLocator input) {
    return deleteStore(new ErStore(input, EMPTY_PARTITION_ARRAY));
  }

  public ErSessionMeta getOrCreateSession(ErSessionMeta sessionMeta) {
    return doSyncRequestInternal(sessionMeta, ErSessionMeta.class, SessionCommands.GET_OR_CREATE_SESSION());
  }

  public ErProcessorBatch getOrCreateProcessorBatch(ErSessionMeta sessionMeta) {
    return doSyncRequestInternal(sessionMeta, ErProcessorBatch.class, SessionCommands.GET_OR_CREATE_PROCESSOR_BATCH());
  }

  private <T> T doSyncRequestInternal(RpcMessage input, Class<T> outputType, CommandURI commandURI) {
    return commandClient.simpleSyncSend(input, outputType, clusterManagerEndpoint, commandURI, SerdesTypes.PROTOBUF());
  }
}
