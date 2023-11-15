package com.eggroll.core.pojo;

import com.eggroll.core.config.Dict;
import com.google.protobuf.ByteString;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class StartDeepspeedContainerRequest {
    private String sessionId;
    private String name;
    private List<String> commandArguments;
    private Map<String, String> environmentVariables;
    private Map<String, byte[]> files;
    private Map<String, byte[]> zippedFiles;
    private Map<Long, byte[]> typedExtraConfigs;
    private Map<String, String> options;
    private Map<Long, DeepspeedContainerConfig> deepspeedConfigs;

    public StartDeepspeedContainerRequest() {
        this.sessionId = Dict.EMPTY;
        this.name = Dict.EMPTY;
        this.commandArguments = new ArrayList<>();
        this.environmentVariables = new HashMap<>();
        this.files = new HashMap<>();
        this.zippedFiles = new HashMap<>();
        this.options = new HashMap<>();
        this.deepspeedConfigs = new HashMap<>();
    }


    public static StartDeepspeedContainerRequest fromStartContainersRequest(StartContainersRequest src) {
        StartDeepspeedContainerRequest dst = new StartDeepspeedContainerRequest();
        dst.sessionId = src.getSessionId();
        dst.name = src.getName();
        dst.commandArguments = src.getCommandArguments();
        dst.environmentVariables = new HashMap<>(src.getEnvironmentVariables());
        dst.files = new HashMap<>(src.getFiles());
        dst.zippedFiles = new HashMap<>(src.getZippedFiles());
        dst.options = new HashMap<>(src.getOptions());
        dst.deepspeedConfigs = new HashMap<>();
        src.getTypedExtraConfigs().forEach((k, v) -> {
            DeepspeedContainerConfig deepspeedContainerConfig = new DeepspeedContainerConfig();
            deepspeedContainerConfig.deserialize(v);
            dst.deepspeedConfigs.put(k, deepspeedContainerConfig);
        });

        return dst;
    }

    public static StartContainersRequest toStartContainersRequest(StartDeepspeedContainerRequest src) {
        StartContainersRequest dst = new StartContainersRequest();
        dst.setSessionId(src.sessionId);
        dst.setName(src.name);
        dst.setJobType(JobProcessorTypes.DeepSpeed.getName());
        dst.setCommandArguments(src.commandArguments);
        dst.setEnvironmentVariables(new HashMap<>(src.environmentVariables));
        dst.setFiles(new HashMap<>(src.files));
        dst.setZippedFiles(new HashMap<>(src.zippedFiles));
        dst.setOptions(new HashMap<>(src.options));
        dst.setTypedExtraConfigs(new HashMap<>());

        for (Map.Entry<Long, DeepspeedContainerConfig> entry : src.deepspeedConfigs.entrySet()) {
            Long key = entry.getKey();
            DeepspeedContainerConfig value = entry.getValue();

            dst.getTypedExtraConfigs().put(key, value.serialize());
        }
        return dst;
    }
}
