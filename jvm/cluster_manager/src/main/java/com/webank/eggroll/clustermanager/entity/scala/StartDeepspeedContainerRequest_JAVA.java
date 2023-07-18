package com.webank.eggroll.clustermanager.entity.scala;

import com.google.protobuf.ByteString;
import com.webank.eggroll.clustermanager.config.DeepspeedContainerConfig_JAVA;
import com.webank.eggroll.core.constant.StringConstants;

import java.util.HashMap;
import java.util.Map;

public class StartDeepspeedContainerRequest_JAVA {
    public String sessionId;
    public String name;
    public String[] commandArguments;
    public Map<String, String> environmentVariables;
    public Map<String, byte[]> files;
    public Map<String, byte[]> zippedFiles;
    public Map<Long, byte[]> typedExtraConfigs;
    public Map<String, String> options;
    public Map<Long, DeepspeedContainerConfig_JAVA> deepspeedConfigs;

    public StartDeepspeedContainerRequest_JAVA() {
        this.sessionId = StringConstants.EMPTY();
        this.name = StringConstants.EMPTY();
        this.commandArguments = new String[0];
        this.environmentVariables = new HashMap<>();
        this.files = new HashMap<>();
        this.zippedFiles = new HashMap<>();
        this.options = new HashMap<>();
        this.deepspeedConfigs = new HashMap<>();
    }

    public static StartDeepspeedContainerRequest_JAVA fromStartContainersRequest(StartDeepspeedContainerRequest_JAVA src) {
        StartDeepspeedContainerRequest_JAVA dst = new StartDeepspeedContainerRequest_JAVA();
        dst.sessionId = src.sessionId;
        dst.name = src.name;
        dst.commandArguments = src.commandArguments;
        dst.environmentVariables = new HashMap<>(src.environmentVariables);
        dst.files = new HashMap<>(src.files);
        dst.zippedFiles = new HashMap<>(src.zippedFiles);
        dst.options = new HashMap<>(src.options);
        dst.deepspeedConfigs = new HashMap<>();
        src.typedExtraConfigs.forEach((k,v)->{
            dst.deepspeedConfigs.put(k, DeepspeedContainerConfig_JAVA.deserialize(ByteString.copyFrom(v)));
        });

        return dst;
    }

    public static StartContainersRequest_JAVA toStartContainersRequest(StartDeepspeedContainerRequest_JAVA src) {
        StartContainersRequest_JAVA dst = new StartContainersRequest_JAVA();
        dst.sessionId = src.sessionId;
        dst.name = src.name;
        dst.jobType = JobProcessorTypes_JAVA.deepSpeed.name();
        dst.commandArguments = src.commandArguments;
        dst.environmentVariables = new HashMap<>(src.environmentVariables);
        dst.files = new HashMap<>(src.files);
        dst.zippedFiles = new HashMap<>(src.zippedFiles);
        dst.options = new HashMap<>(src.options);
        dst.typedExtraConfigs = new HashMap<>();

        for (Map.Entry<Long, DeepspeedContainerConfig_JAVA> entry : src.deepspeedConfigs.entrySet()) {
            Long key = entry.getKey();
            DeepspeedContainerConfig_JAVA value = entry.getValue();
            dst.typedExtraConfigs.put(key, DeepspeedContainerConfig_JAVA.serialize(value));
        }

        return dst;
    }
}
