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

package org.fedai.eggroll.core.grpc;

import org.fedai.eggroll.core.containers.meta.KillContainersResponse;
import org.fedai.eggroll.core.containers.meta.StartContainersResponse;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.fedai.eggroll.core.grpc.CommandUri.*;


public class NodeManagerClient {
    Logger logger = LoggerFactory.getLogger(NodeManagerClient.class);

    CommandClient commandClient;
    ErEndpoint nodeManagerEndpoint;

    public NodeManagerClient(ErEndpoint nodeManagerEndpoint) {
        if (nodeManagerEndpoint == null) {
            throw new IllegalArgumentException("failed to create NodeManagerClient for endpoint: " + nodeManagerEndpoint);
        }
        this.nodeManagerEndpoint = nodeManagerEndpoint;
        commandClient = new CommandClient();
    }

    public ErSessionMeta startContainers(Context context, ErSessionMeta sessionMeta) {
        byte[] responseData = commandClient.call(context, nodeManagerEndpoint, startContainers, sessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public ErSessionMeta stopContainers(Context context, ErSessionMeta sessionMeta) {
        byte[] responseData = commandClient.call(context, nodeManagerEndpoint, stopContainers, sessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }


    public StartContainersResponse startFlowJobContainers(Context context, StartFlowContainersRequest startFlowContainersRequest) {
        byte[] responseData = commandClient.call(context, nodeManagerEndpoint, startFlowJobContainers, startFlowContainersRequest.serialize());
        StartContainersResponse response = new StartContainersResponse();
        response.deserialize(responseData);
        return response;
    }

    public StartContainersResponse startJobContainers(Context context, StartContainersRequest startContainersRequest) {
        byte[] responseData = commandClient.call(context, nodeManagerEndpoint, startJobContainers, startContainersRequest.serialize());
        StartContainersResponse response = new StartContainersResponse();
        response.deserialize(responseData);
        return response;
    }


    public ErSessionMeta killContainers(Context context, ErSessionMeta sessionMeta) {
        byte[] responseData = commandClient.call(context, nodeManagerEndpoint, killContainers, sessionMeta.serialize());
        ErSessionMeta response = new ErSessionMeta();
        response.deserialize(responseData);
        return response;
    }

    public KillContainersResponse killJobContainers(Context context, KillContainersRequest killContainersRequest) {
        byte[] responseData = commandClient.call(context, nodeManagerEndpoint, killJobContainers, killContainersRequest.serialize());
        KillContainersResponse response = new KillContainersResponse();
        response.deserialize(responseData);
        return response;
    }

    public ErProcessor checkNodeProcess(Context context, ErProcessor processor) {
        byte[] responseData = commandClient.call(context, nodeManagerEndpoint, checkNodeProcess, processor.serialize());
        ErProcessor response = new ErProcessor();
        response.deserialize(responseData);
        return response;
    }

    public DownloadContainersResponse downloadContainers(Context context, DownloadContainersRequest downloadContainersRequest) {
        byte[] responseData = commandClient.call(context, nodeManagerEndpoint, downloadContainers, downloadContainersRequest.serialize());
        DownloadContainersResponse response = new DownloadContainersResponse();
        response.deserialize(responseData);
        return response;
    }


    public MetaInfoResponse queryNodeMetaInfo(Context context, MetaInfoRequest metaInfoRequest) {
        byte[] responseData = commandClient.call(context, nodeManagerEndpoint, nodeMetaInfo,metaInfoRequest.serialize());
        MetaInfoResponse response = new MetaInfoResponse();
        response.deserialize(responseData);
        return response;
    }

}
