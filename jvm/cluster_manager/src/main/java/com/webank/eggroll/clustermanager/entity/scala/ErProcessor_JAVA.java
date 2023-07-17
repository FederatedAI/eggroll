package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.meta.NetworkingRpcMessage;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;

@Data
public class ErProcessor_JAVA implements NetworkingRpcMessage_JAVA {
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

    public ErProcessor(long id, String sessionId, long serverNodeId, String name, String processorType,
                       String status, ErEndpoint commandEndpoint, ErEndpoint transferEndpoint, int pid,
                       Map<String, String> options, String tag, ErResource[] resources,
                       Timestamp createdAt, Timestamp updatedAt) {
        this.id = id;
        this.sessionId = sessionId;
        this.serverNodeId = serverNodeId;
        this.name = name;
        this.processorType = processorType;
        this.status = status;
        this.commandEndpoint = commandEndpoint;
        this.transferEndpoint = transferEndpoint;
        this.pid = pid;
        this.options = new ConcurrentHashMap<>(options);
        this.tag = tag;
        this.resources = resources;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }
}