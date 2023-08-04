package com.eggroll.core.grpc;

public class CommandUri {

    public  static  final String     getServerNode= "v1/cluster-manager/metadata/getServerNode";
    public  static  final String     getServerNodes = "v1/cluster-manager/metadata/getServerNodes";
    public  static  final String     getOrCreateServerNode =  "v1/cluster-manager/metadata/getOrCreateServerNode";
    public  static  final String     createOrUpdateServerNode = "v1/cluster-manager/metadata/createOrUpdateServerNode";
    public  static  final String     getStore = "v1/cluster-manager/metadata/getStore";
    public  static  final String     getOrCreateStore = "v1/cluster-manager/metadata/getOrCreateStore";

    public  static  final String     deleteStore = "v1/cluster-manager/metadata/deleteStore";
    public  static  final String     getStoreFromNamespace = "v1/cluster-manager/metadata/getStoreFromNamespace";
    public  static  final String     getOrCreateSession = "v1/cluster-manager/session/getOrCreateSession";
    public  static  final String    getSession = "v1/cluster-manager/session/getSession";
    public  static  final String    heartbeat = "v1/cluster-manager/session/heartbeat";
    public  static  final String  nodeHeartbeat = "v1/cluster-manager/manager/nodeHeartbeat";
    public static   final String  stopSession = "v1/cluster-manager/session/stopSession";

    public static   final String  killSession= "v1/cluster-manager/session/killSession";
    public static   final String  killAllSessions = "v1/cluster-manager/session/killAllSessions";

    public static final String startContainers = "v1/node_manager/processor/startContainers";
    public static final String stopContainers = "v1/node_manager/processor/stopContainers";
    public static final String killContainers = "v1/node_manager/processor/killContainers";
    public static final String eggpairHeartbeat = "v1/node_manager/processor/heartbeat";
}
