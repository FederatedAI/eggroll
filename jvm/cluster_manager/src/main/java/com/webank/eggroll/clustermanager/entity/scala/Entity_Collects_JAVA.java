package com.webank.eggroll.clustermanager.entity.scala;


import com.webank.eggroll.core.constant.ResourceStatus;
import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.Meta;
import com.webank.eggroll.core.meta.NetworkingRpcMessage;

import org.apache.commons.lang3.StringUtils;

import java.security.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Entity_Collects_JAVA {




    public static class ErEndpoint   {
        private String host;
        private int port;

        public ErEndpoint(String url) {
            String[] toks = url.split(":");
            this.host = toks[0];
            this.port = Integer.parseInt(toks[1]);
        }

        public ErEndpoint(String host , Integer port){
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }

        public boolean isValid() {
            return !StringUtils.isBlank(host) && port > 0;
        }

        public Meta.Endpoint toProto() {
            Meta.Endpoint.Builder builder = Meta.Endpoint.newBuilder()
                    .setHost(host)
                    .setPort(port);

            return builder.build();
        }
    }


    public class ErResource  {
        private long resourceId;
        private String resourceType;
        private long serverNodeId;
        private long total;
        private long used;
        private long allocated;
        private long preAllocated;
        private String extention;
        private String status;
        private List<String> extentionCache;

        public ErResource() {
            this.resourceId = -1;
            this.resourceType = StringConstants.EMPTY();
            this.serverNodeId = 0;
            this.total = -1;
            this.used = -1;
            this.allocated = -1;
            this.preAllocated = -1;
            this.extention = null;
            this.status = ResourceStatus.AVAILABLE();
            this.extentionCache = new ArrayList<>();
        }

        public long getUnAllocatedResource() {
            long remain = total;
            if (allocated > 0) {
                remain = remain - allocated;
            }
            if (preAllocated > 0) {
                remain = remain - preAllocated;
            }
            return remain;
        }
    }


    public class ErResourceAllocation   {
        private long serverNodeId;
        private String sessionId;
        private String operateType;
        private String status;
        private ErResource[] resources;

        public long getServerNodeId() {
            return serverNodeId;
        }

        public void setServerNodeId(long serverNodeId) {
            this.serverNodeId = serverNodeId;
        }

        public String getSessionId() {
            return sessionId;
        }

        public void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }

        public String getOperateType() {
            return operateType;
        }

        public void setOperateType(String operateType) {
            this.operateType = operateType;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public ErResource[] getResources() {
            return resources;
        }

        public void setResources(ErResource[] resources) {
            this.resources = resources;
        }

        public ErResourceAllocation(long serverNodeId, String sessionId, String operateType, String status, ErResource[] resources) {
            this.serverNodeId = serverNodeId;
            this.sessionId = sessionId;
            this.operateType = operateType;
            this.status = status;
            this.resources = resources;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (ErResource r : resources) {
                sb.append("[");
                sb.append(r.toString());
                sb.append("]");
            }
            return String.format("<ErResourceAllocation(serverNodeId=%d, sessionId=%s, operateType=%s, status=%s, resources=%s)>",
                    serverNodeId, sessionId, operateType, status, sb.toString());
        }
    }


    public class ErProcessor {
        private long id;
        private String sessionId;
        private long serverNodeId;
        private String name;
        private String processorType;
        private String status;
        private ErEndpoint commandEndpoint;
        private ErEndpoint transferEndpoint;
        private int pid;
        private Map<String, String> options;
        private String tag;
        private ErResource[] resources;
        private Timestamp createdAt;
        private Timestamp updatedAt;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getSessionId() {
            return sessionId;
        }

        public void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }

        public long getServerNodeId() {
            return serverNodeId;
        }

        public void setServerNodeId(long serverNodeId) {
            this.serverNodeId = serverNodeId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getProcessorType() {
            return processorType;
        }

        public void setProcessorType(String processorType) {
            this.processorType = processorType;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public ErEndpoint getCommandEndpoint() {
            return commandEndpoint;
        }

        public void setCommandEndpoint(ErEndpoint commandEndpoint) {
            this.commandEndpoint = commandEndpoint;
        }

        public ErEndpoint getTransferEndpoint() {
            return transferEndpoint;
        }

        public void setTransferEndpoint(ErEndpoint transferEndpoint) {
            this.transferEndpoint = transferEndpoint;
        }

        public int getPid() {
            return pid;
        }

        public void setPid(int pid) {
            this.pid = pid;
        }

        public Map<String, String> getOptions() {
            return options;
        }

        public void setOptions(Map<String, String> options) {
            this.options = options;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public ErResource[] getResources() {
            return resources;
        }

        public void setResources(ErResource[] resources) {
            this.resources = resources;
        }

        public Timestamp getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(Timestamp createdAt) {
            this.createdAt = createdAt;
        }

        public Timestamp getUpdatedAt() {
            return updatedAt;
        }

        public void setUpdatedAt(Timestamp updatedAt) {
            this.updatedAt = updatedAt;
        }

        public ErProcessor() {
            this.id = -1;
            this.sessionId = StringConstants.EMPTY();
            this.serverNodeId = -1;
            this.name = StringConstants.EMPTY();
            this.processorType = StringConstants.EMPTY();
            this.status = StringConstants.EMPTY();
            this.commandEndpoint = null;
            this.transferEndpoint = null;
            this.pid = -1;
            this.options = new ConcurrentHashMap<>();
            this.tag = StringConstants.EMPTY();
            this.resources = new ErResource[0];
            this.createdAt = null;
            this.updatedAt = null;
        }


        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (resources != null) {
                for (ErResource resource : resources) {
                    sb.append(resource.toString());
                }
            }

            return "<ErProcessor(id=" + id + ", sessionId=" + sessionId +
                    ", serverNodeId=" + serverNodeId + ", name=" + name +
                    ", processorType=" + processorType + ", status=" + status +
                    ", commandEndpoint=" + commandEndpoint + ", transferEndpoint=" + transferEndpoint +
                    ", createdAt=" + createdAt + ", updatedAt=" + updatedAt +
                    ", pid=" + pid + ", options=" + options + ", tag=" + tag +
                    ") at " + Integer.toHexString(hashCode()) + " resources " + sb.toString() + ">";
        }

        public Meta.Processor toProto() {
            Meta.Processor.Builder builder = Meta.Processor.newBuilder()
                    .setId(this.getId())
                    .setServerNodeId(this.getServerNodeId())
                    .setName(this.getName())
                    .setProcessorType(this.getProcessorType())
                    .setStatus(this.getStatus())
                    .setCommandEndpoint(this.getCommandEndpoint() != null ? this.getCommandEndpoint().toProto() : Meta.Endpoint.getDefaultInstance())
                    .setTransferEndpoint(this.getTransferEndpoint() != null ? this.getTransferEndpoint().toProto() : Meta.Endpoint.getDefaultInstance())
                    .setPid(this.getPid())
                    .putAllOptions(this.getOptions())
                    .setTag(this.getTag());

            return builder.build();
        }

    }



    public class ErProcessorBatch  {
        private long id;
        private String name;
        private ErProcessor[] processors;
        private String tag;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public ErProcessor[] getProcessors() {
            return processors;
        }

        public void setProcessors(ErProcessor[] processors) {
            this.processors = processors;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public ErProcessorBatch() {
            this.id = -1;
            this.name = StringConstants.EMPTY();
            this.processors = new ErProcessor[0];
            this.tag = StringConstants.EMPTY();
        }

        public ErProcessorBatch(long id, String name, ErProcessor[] processors, String tag) {
            this.id = id;
            this.name = name;
            this.processors = processors;
            this.tag = tag;
        }

        @Override
        public String toString() {
            return "<ErProcessorBatch(id=" + id + ", name=" + name +
                    ", processors=" + Arrays.toString(processors) + ", tag=" + tag +
                    ") at " + Integer.toHexString(hashCode()) + ">";
        }

        public Meta.ProcessorBatch toProto() {
            Meta.ProcessorBatch.Builder builder = Meta.ProcessorBatch.newBuilder();
            builder.setId(this.getId())
                    .setName(this.getName())
                    .addAllProcessors(Arrays.stream(this.getProcessors()).map(ErProcessor::toProto).collect(Collectors.toList()))
                    .setTag(this.getTag());
            return builder.build();
        }
    }

    public class ErNodeHeartbeat {
        private long id;
        private ErServerNode node;

        public ErNodeHeartbeat() {
            this.id = -1;
            this.node = null;
        }

        public ErNodeHeartbeat(long id, ErServerNode node) {
            this.id = id;
            this.node = node;
        }

        @Override
        public String toString() {
            return "<ErNodeHeartbeat(id=" + id + ", node=" + node +
                    ") at " + Integer.toHexString(hashCode()) + ">";
        }
    }


    public class ErServerNode {
        private long id;
        private String name;
        private long clusterId;
        private ErEndpoint endpoint;
        private String nodeType;
        private String status;
        private Timestamp lastHeartBeat;
        private ErResource[] resources;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getClusterId() {
            return clusterId;
        }

        public void setClusterId(long clusterId) {
            this.clusterId = clusterId;
        }

        public ErEndpoint getEndpoint() {
            return endpoint;
        }

        public void setEndpoint(ErEndpoint endpoint) {
            this.endpoint = endpoint;
        }

        public String getNodeType() {
            return nodeType;
        }

        public void setNodeType(String nodeType) {
            this.nodeType = nodeType;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Timestamp getLastHeartBeat() {
            return lastHeartBeat;
        }

        public void setLastHeartBeat(Timestamp lastHeartBeat) {
            this.lastHeartBeat = lastHeartBeat;
        }

        public ErResource[] getResources() {
            return resources;
        }

        public void setResources(ErResource[] resources) {
            this.resources = resources;
        }

        public ErServerNode() {
            this.id = -1;
            this.name = StringConstants.EMPTY();
            this.clusterId = 0;
            this.endpoint = new ErEndpoint(StringConstants.EMPTY(), -1);
            this.nodeType = StringConstants.EMPTY();
            this.status = StringConstants.EMPTY();
            this.lastHeartBeat = null;
            this.resources = new ErResource[0];
        }

        public ErServerNode(long id, String name, long clusterId, ErEndpoint endpoint,
                            String nodeType, String status, Timestamp lastHeartBeat,
                            ErResource[] resources) {
            this.id = id;
            this.name = name;
            this.clusterId = clusterId;
            this.endpoint = endpoint;
            this.nodeType = nodeType;
            this.status = status;
            this.lastHeartBeat = lastHeartBeat;
            this.resources = resources;
        }

        public ErServerNode(String nodeType, String status) {
            this.id = -1;
            this.name = StringConstants.EMPTY();
            this.clusterId = 0;
            this.endpoint = new ErEndpoint(StringConstants.EMPTY(), -1);
            this.nodeType = nodeType;
            this.status = status;
            this.lastHeartBeat = null;
            this.resources = new ErResource[0];
        }

        @Override
        public String toString() {
            return "<ErServerNode(id=" + id + ", name=" + name +
                    ", clusterId=" + clusterId + ", endpoint=" + endpoint +
                    ", nodeType=" + nodeType + ", status=" + status +
                    ", lastHeartBeat=" + lastHeartBeat +
                    ", resources=" + Arrays.toString(resources) +
                    ") at " + Integer.toHexString(hashCode()) + ">";
        }
    }


    public class ErServerCluster {
        private long id;
        private String name;
        private ErServerNode[] serverNodes;
        private String tag;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public ErServerNode[] getServerNodes() {
            return serverNodes;
        }

        public void setServerNodes(ErServerNode[] serverNodes) {
            this.serverNodes = serverNodes;
        }

        public String getTag() {
            return tag;
        }

        public void setTag(String tag) {
            this.tag = tag;
        }

        public ErServerCluster() {
            this.id = -1;
            this.name = StringConstants.EMPTY();
            this.serverNodes = new ErServerNode[0];
            this.tag = StringConstants.EMPTY();
        }

        public ErServerCluster(long id, ErServerNode[] serverNodes, String tag) {
            this.id = id;
            this.name = StringConstants.EMPTY();
            this.serverNodes = serverNodes;
            this.tag = tag;
        }

        @Override
        public String toString() {
            return "<ErServerCluster(id=" + id + ", name=" + name +
                    ", serverNodes=" + Arrays.toString(serverNodes) + ", tag=" + tag +
                    ") at " + Integer.toHexString(hashCode()) + ">";
        }
    }





}
