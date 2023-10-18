package com.eggroll.core.pojo;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class StartFlowContainersRequest implements RpcMessage{
    public String sessionId;
    public String name;
    public List<String> commandArguments;
    public Map<String, String> environmentVariables;
    public Map<String, String> options;
    public List<ErProcessor> processors;

    @Override
    public byte[] serialize() {
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] data) {

    }
}
