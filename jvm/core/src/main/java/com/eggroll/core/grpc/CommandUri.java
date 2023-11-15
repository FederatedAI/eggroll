package com.eggroll.core.grpc;

import lombok.val;

public class CommandUri {

    public static final String getServerNode = "v1/cluster-manager/metadata/getServerNode";
    public static final String getServerNodes = "v1/cluster-manager/metadata/getServerNodes";
    public static final String getOrCreateServerNode = "v1/cluster-manager/metadata/getOrCreateServerNode";
    public static final String createOrUpdateServerNode = "v1/cluster-manager/metadata/createOrUpdateServerNode";
    public static final String getStore = "v1/cluster-manager/metadata/getStore";
    public static final String getOrCreateStore = "v1/cluster-manager/metadata/getOrCreateStore";

    public static final String deleteStore = "v1/cluster-manager/metadata/deleteStore";
    public static final String getStoreFromNamespace = "v1/cluster-manager/metadata/getStoreFromNamespace";
    public static final String getOrCreateSession = "v1/cluster-manager/session/getOrCreateSession";
    public static final String getSession = "v1/cluster-manager/session/getSession";
    public static final String heartbeat = "v1/cluster-manager/session/heartbeat";
    public static final String nodeHeartbeat = "v1/cluster-manager/manager/nodeHeartbeat";
    public static final String stopSession = "v1/cluster-manager/session/stopSession";
    public static final String nodeMetaInfo = "v1/cluster-manager/manager/nodeMetaInfo";

    public static final String killSession = "v1/cluster-manager/session/killSession";
    public static final String killAllSessions = "v1/cluster-manager/session/killAllSessions";

    public static final String getQueueView = "v1/cluster-manager/session/getQueueView";

    public static final String startContainers = "v1/node-manager/processor/startContainers";
    public static final String stopContainers = "v1/node-manager/processor/stopContainers";
    public static final String killContainers = "v1/node-manager/processor/killContainers";
    public static final String eggpairHeartbeat = "v1/node-manager/processor/heartbeat";

    public static final String checkNodeProcess = "v1/node-manager/resouce/checkNodeProcess";

    public static final String startFlowJobContainers = "v1/node-manager/container/startFlowJobContainers";

    public static final String startJobContainers = "v1/node-manager/container/startJobContainers";
    public static final String stopJobContainers = "v1/node-manager/container/stopJobContainers";
    public static final String killJobContainers = "v1/node-manager/container/killJobContainers";
    public static final String downloadContainers = "v1/node-manager/container/downloadContainers";

    public static final String submitJob = "v1/cluster-manager/job/submitJob";
    public static final String queryJobStatus = "v1/cluster-manager/job/queryJobStatus";
    public static final String queryJob = "v1/cluster-manager/job/queryJob";
    public static final String stopJob = "v1/cluster-manager/job/stopJob";
    public static final String killJob = "v1/cluster-manager/job/killJob";
    public static final String downloadJob = "v1/cluster-manager/job/downloadJob";
    public static final String prepareJobDownload = "v1/cluster-manager/job/prepareJobDownload";

    public static final String rendezvousAdd = "v1/cluster-manager/job/rendezvous/add";
    public static final String rendezvousDestroy = "v1/cluster-manager/job/rendezvous/destroy";
    public static final String rendezvousGet = "v1/cluster-manager/job/rendezvous/get";
    public static final String rendezvousSet = "v1/cluster-manager/job/rendezvous/set";


    public static final String checkResourceEnough = "v1/cluster-manager/resource/checkResourceEnough";


}
