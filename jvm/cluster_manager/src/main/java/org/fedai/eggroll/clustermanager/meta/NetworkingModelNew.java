//package com.webank.eggroll.clustermanager.meta;
//
//import com.webank.eggroll.core.constant.ResourceStatus;
//import com.webank.eggroll.core.constant.StringConstants;
//import com.webank.eggroll.core.datastructure.RpcMessage;
//import com.webank.eggroll.core.meta.Meta;
//import com.webank.eggroll.core.serdes.BaseSerializable;
//import com.webank.eggroll.core.serdes.PbMessageDeserializer;
//import com.webank.eggroll.core.serdes.PbMessageSerializer;
//import org.apache.commons.lang3.StringUtils;
//
//import java.security.Timestamp;
//import java.util.*;
//import java.util.stream.Collectors;
//
//public class NetworkingModelNew {
//
//
//    public interface NetworkingRpcMessage extends RpcMessage {
//        @Override
//        default String rpcMessageType() {
//            return "Networking";
//        }
//    }
//
//    public static class ErEndpoint implements NetworkingRpcMessage {
//        private final String host;
//        private final int port;
//
//        public ErEndpoint(String host, int port) {
//            this.host = host;
//            this.port = port;
//        }
//
//        public String getHost() {
//            return host;
//        }
//
//        public int getPort() {
//            return port;
//        }
//
//        @Override
//        public String toString() {
//            return host + ":" + port;
//        }
//
//        public boolean isValid() {
//            return !StringUtils.isBlank(host) && port > 0;
//        }
//
//        public static ErEndpoint apply(String url) {
//            String[] toks = url.split(":");
//            return new ErEndpoint(toks[0], Integer.parseInt(toks[1]));
//        }
//    }
//
//    public static class ErResource implements NetworkingRpcMessage {
//        private long resourceId;
//        private String resourceType;
//        private long serverNodeId;
//        private long total;
//        private long used;
//        private long allocated;
//        private long preAllocated;
//        private String extension;
//        private String status;
//        private List<String> extensionCache;
//
//        public ErResource() {
//            this.resourceId = -1;
//            this.resourceType = StringConstants.EMPTY();
//            this.serverNodeId = 0;
//            this.total = -1;
//            this.used = -1;
//            this.allocated = -1;
//            this.preAllocated = -1;
//            this.extension = null;
//            this.status = ResourceStatus.AVAILABLE();
//            this.extensionCache = new ArrayList<>();
//        }
//
//        public ErResource(long resourceId, String resourceType, long serverNodeId, long total, long used, long allocated,
//                          long preAllocated, String extension, String status, List<String> extensionCache) {
//            this.resourceId = resourceId;
//            this.resourceType = resourceType;
//            this.serverNodeId = serverNodeId;
//            this.total = total;
//            this.used = used;
//            this.allocated = allocated;
//            this.preAllocated = preAllocated;
//            this.extension = extension;
//            this.status = status;
//            this.extensionCache = extensionCache;
//        }
//
//        public long getResourceId() {
//            return resourceId;
//        }
//
//        public void setResourceId(long resourceId) {
//            this.resourceId = resourceId;
//        }
//
//        public String getResourceType() {
//            return resourceType;
//        }
//
//        public void setResourceType(String resourceType) {
//            this.resourceType = resourceType;
//        }
//
//        public long getServerNodeId() {
//            return serverNodeId;
//        }
//
//        public void setServerNodeId(long serverNodeId) {
//            this.serverNodeId = serverNodeId;
//        }
//
//        public long getTotal() {
//            return total;
//        }
//
//        public void setTotal(long total) {
//            this.total = total;
//        }
//
//        public long getUsed() {
//            return used;
//        }
//
//        public void setUsed(long used) {
//            this.used = used;
//        }
//
//        public long getAllocated() {
//            return allocated;
//        }
//
//        public void setAllocated(long allocated) {
//            this.allocated = allocated;
//        }
//
//        public long getPreAllocated() {
//            return preAllocated;
//        }
//
//        public void setPreAllocated(long preAllocated) {
//            this.preAllocated = preAllocated;
//        }
//
//        public String getExtension() {
//            return extension;
//        }
//
//        public void setExtension(String extension) {
//            this.extension = extension;
//        }
//
//        public String getStatus() {
//            return status;
//        }
//
//        public void setStatus(String status) {
//            this.status = status;
//        }
//
//        public List<String> getExtensionCache() {
//            return extensionCache;
//        }
//
//        public void setExtensionCache(List<String> extensionCache) {
//            this.extensionCache = extensionCache;
//        }
//
//        @Override
//        public String toString() {
//            return "<ErResource(resourceType=" + resourceType + ", status=" + status + ", total=" + total + ", used=" +
//                    used + ", allocated=" + allocated + ", preAllocated=" + preAllocated + ", extension=" + extension + ")>";
//        }
//
//        public long getUnAllocatedResource() {
//            long remain = total;
//            if (allocated > 0) {
//                remain = remain - allocated;
//            }
//            if (preAllocated > 0) {
//                remain = remain - preAllocated;
//            }
//            return remain;
//        }
//    }
//
//    public class ErResourceAllocation implements NetworkingRpcMessage {
//        private long serverNodeId;
//        private String sessionId;
//        private String operateType;
//        private String status;
//        private ErResource[] resources;
//
//        public ErResourceAllocation(long serverNodeId, String sessionId, String operateType, String status, ErResource[] resources) {
//            this.serverNodeId = serverNodeId;
//            this.sessionId = sessionId;
//            this.operateType = operateType;
//            this.status = status;
//            this.resources = resources;
//        }
//
//        @Override
//        public String toString() {
//            StringBuilder sb = new StringBuilder();
//            for (ErResource r : resources) {
//                sb.append("[");
//                sb.append(r.toString());
//                sb.append("]");
//            }
//            return "<ErResourceAllocation(serverNodeId=" + serverNodeId +
//                    ", sessionId=" + sessionId +
//                    ", operateType=" + operateType +
//                    ", status=" + status +
//                    ", resources=" + sb.toString() +
//                    ")>";
//        }
//    }
//
//    public static class ErProcessor implements NetworkingRpcMessage {
//        private long id;
//        private String sessionId;
//        private long serverNodeId;
//        private String name;
//        private String processorType;
//        private String status;
//        private ErEndpoint commandEndpoint;
//        private ErEndpoint transferEndpoint;
//        private int pid;
//        private Map<String, String> options;
//        private String tag;
//        private ErResource[] resources;
//        private Timestamp createdAt;
//        private Timestamp updatedAt;
//
//        public ErProcessor(long id, String sessionId, long serverNodeId, String name, String processorType, String status,
//                           ErEndpoint commandEndpoint, ErEndpoint transferEndpoint, int pid,
//                           Map<String, String> options, String tag, ErResource[] resources,
//                           Timestamp createdAt, Timestamp updatedAt) {
//            this.id = id;
//            this.sessionId = sessionId;
//            this.serverNodeId = serverNodeId;
//            this.name = name;
//            this.processorType = processorType;
//            this.status = status;
//            this.commandEndpoint = commandEndpoint;
//            this.transferEndpoint = transferEndpoint;
//            this.pid = pid;
//            this.options = options;
//            this.tag = tag;
//            this.resources = resources;
//            this.createdAt = createdAt;
//            this.updatedAt = updatedAt;
//        }
//
//        public static ErProcessor createProcessorWithIdAndServerNodeId(long id, long serverNodeId) {
//            return new ErProcessor(id, "", serverNodeId, "", "", "", null, null,
//                    -1, new HashMap<>(), "", new ErResource[0], null, null);
//        }
//
//        public static ErProcessor createProcessorWithIdServerNodeIdAndTag(long id, long serverNodeId, String tag) {
//            return new ErProcessor(id, "", serverNodeId, "", "", "", null, null,
//                    -1, new HashMap<>(), tag, new ErResource[0], null, null);
//        }
//
//        @Override
//        public String toString() {
//            StringBuilder sb = new StringBuilder();
//            if (resources != null) {
//                for (ErResource r : resources) {
//                    sb.append(r.toString());
//                }
//            }
//            return "<ErProcessor(id=" + id +
//                    ", sessionId=" + sessionId +
//                    ", serverNodeId=" + serverNodeId +
//                    ", name=" + name +
//                    ", processorType=" + processorType +
//                    ", status=" + status +
//                    ", commandEndpoint=" + commandEndpoint +
//                    ", transferEndpoint=" + transferEndpoint +
//                    ", pid=" + pid +
//                    ", options=" + options +
//                    ", tag=" + tag +
//                    ", createdAt=" + createdAt +
//                    ", updatedAt=" + updatedAt +
//                    ", resources" + sb.toString() +
//                    ") at " + Integer.toHexString(hashCode()) + ">";
//        }
//    }
//
//    public class ErProcessorBatch implements NetworkingRpcMessage {
//        private long id;
//        private String name;
//        private ErProcessor[] processors;
//        private String tag;
//
//        public ErProcessorBatch(long id, String name, ErProcessor[] processors, String tag) {
//            this.id = id;
//            this.name = name;
//            this.processors = processors;
//            this.tag = tag;
//        }
//
//        @Override
//        public String toString() {
//            return "<ErProcessorBatch(id=" + id +
//                    ", name=" + name +
//                    ", processors=" + Arrays.toString(processors) +
//                    ", tag=" + tag +
//                    ")>";
//        }
//    }
//
//    public class ErNodeHeartbeat implements NetworkingRpcMessage {
//        private long id;
//        private ErServerNode node;
//
//        public ErNodeHeartbeat(long id, ErServerNode node) {
//            this.id = id;
//            this.node = node;
//        }
//
//        @Override
//        public String toString() {
//            return "<ErNodeHeartbeat(id=" + id +
//                    ", node=" + node +
//                    ")>";
//        }
//    }
//
//    public static class ErServerNode implements NetworkingRpcMessage {
//        private long id;
//        private String name;
//        private long clusterId;
//        private ErEndpoint endpoint;
//        private String nodeType;
//        private String status;
//        private Timestamp lastHeartBeat;
//        private ErResource[] resources;
//
//        public ErServerNode(long id, String name, long clusterId, ErEndpoint endpoint,
//                            String nodeType, String status, Timestamp lastHeartBeat, ErResource[] resources) {
//            this.id = id;
//            this.name = name;
//            this.clusterId = clusterId;
//            this.endpoint = endpoint;
//            this.nodeType = nodeType;
//            this.status = status;
//            this.lastHeartBeat = lastHeartBeat;
//            this.resources = resources;
//        }
//
//        public static ErServerNode createWithNodeTypeAndStatus(String nodeType, String status) {
//            return new ErServerNode(-1, "", 0, null, nodeType, status, null, new ErResource[0]);
//        }
//
//        @Override
//        public String toString() {
//            return "<ErServerNode(id=" + id +
//                    ", name=" + name +
//                    ", clusterId=" + clusterId +
//                    ", endpoint=" + endpoint +
//                    ", nodeType=" + nodeType +
//                    ", status=" + status +
//                    ", lastHeartBeat=" + lastHeartBeat +
//                    ", resources=" + Arrays.toString(resources) +
//                    ")>";
//        }
//    }
//
//    public class ErServerCluster implements NetworkingRpcMessage {
//        private long id;
//        private String name;
//        private ErServerNode[] serverNodes;
//        private String tag;
//
//        public ErServerCluster(long id, String name, ErServerNode[] serverNodes, String tag) {
//            this.id = id;
//            this.name = name;
//            this.serverNodes = serverNodes;
//            this.tag = tag;
//        }
//
//        @Override
//        public String toString() {
//            return "<ErServerCluster(id=" + id +
//                    ", name=" + name +
//                    ", serverNodes=" + Arrays.toString(serverNodes) +
//                    ", tag=" + tag +
//                    ")>";
//        }
//    }
//
//    public class ErEndpointToPbMessage implements PbMessageSerializer {
//        private ErEndpoint src;
//
//        public ErEndpointToPbMessage(ErEndpoint src) {
//            this.src = src;
//        }
//
//        @Override
//        public Meta.Endpoint toProto() {
//            Meta.Endpoint.Builder builder = Meta.Endpoint.newBuilder()
//                    .setHost(src.getHost())
//                    .setPort(src.getPort());
//
//            return builder.build();
//        }
//
//        @Override
//        public byte[] toBytes(BaseSerializable baseSerializable) {
//            return ((ErEndpoint) baseSerializable).toBytes();
//        }
//    }
//
//    public class ErProcessorToPbMessage implements PbMessageSerializer {
//        private ErProcessor src;
//
//        public ErProcessorToPbMessage(ErProcessor src) {
//            this.src = src;
//        }
//
//        @Override
//        public Meta.Processor toProto() {
//            Meta.Processor.Builder builder = Meta.Processor.newBuilder()
//                    .setId(src.id)
//                    .setServerNodeId(src.serverNodeId)
//                    .setName(src.name)
//                    .setProcessorType(src.processorType)
//                    .setStatus(src.status)
//                    .setCommandEndpoint(src.commandEndpoint != null ? src.commandEndpoint.toProto() : Meta.Endpoint.getDefaultInstance())
//                    .setTransferEndpoint(src.transferEndpoint != null ? src.transferEndpoint.toProto() : Meta.Endpoint.getDefaultInstance())
//                    .setPid(src.pid)
//                    .putAllOptions(src.options)
//                    .setTag(src.tag);
//
//            return builder.build();
//        }
//
//        @Override
//        public byte[] toBytes(BaseSerializable baseSerializable) {
//            return baseSerializable.asInstanceOf<ErProcessor>().toBytes();
//        }
//    }
//
//    public class ErProcessorBatchToPbMessage implements PbMessageSerializer {
//        private ErProcessorBatch src;
//
//        public ErProcessorBatchToPbMessage(ErProcessorBatch src) {
//            this.src = src;
//        }
//
//        @Override
//        public Meta.ProcessorBatch toProto() {
//            Meta.ProcessorBatch.Builder builder = Meta.ProcessorBatch.newBuilder()
//                    .setId(src.id)
//                    .setName(src.name)
//                    .addAllProcessors(src.processors.stream().map(Processor::toProto).collect(Collectors.toList()))
//                    .setTag(src.tag);
//
//            return builder.build();
//        }
//
//        @Override
//        public byte[] toBytes(BaseSerializable baseSerializable) {
//            return baseSerializable.asInstanceOf<ErProcessorBatch>().toBytes();
//        }
//    }
//
//    public class ErResourceAllocationFromPbMessage implements PbMessageDeserializer {
//        private Meta.ResourceAllocation src;
//
//        public ErResourceAllocationFromPbMessage(Meta.ResourceAllocation src) {
//            this.src = src;
//        }
//
//        @Override
//        public ErResourceAllocation fromProto() {
//            List<ErResource> resources = src.getResourcesList().stream()
//                    .map(ErResource::fromProto).collect(Collectors.toList());
//
//            return new ErResourceAllocation(src.getServerNodeId(), src.getStatus(),
//                    src.getSessionId(), src.getOperateType(), resources.toArray(new ErResource[0]));
//        }
//
//        @Override
//        public ErResourceAllocation fromBytes(byte[] bytes) {
//            return Meta.ResourceAllocation.parseFrom(bytes).fromProto();
//        }
//    }
//}
