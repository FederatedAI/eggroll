//package com.eggroll.core.pojo;
//
//import com.eggroll.core.config.DeepspeedContainerConfig;
//import com.eggroll.core.config.Dict;
//import com.eggroll.core.constant.StringConstants;
//import com.google.protobuf.ByteString;
//
//import java.util.HashMap;
//import java.util.Map;
//
//public class StartDeepspeedContainerRequest {
//    public String sessionId;
//    public String name;
//    public String[] commandArguments;
//    public Map<String, String> environmentVariables;
//    public Map<String, byte[]> files;
//    public Map<String, byte[]> zippedFiles;
//    public Map<Long, byte[]> typedExtraConfigs;
//    public Map<String, String> options;
//    public Map<Long, DeepspeedContainerConfig> deepspeedConfigs;
//
//    public StartDeepspeedContainerRequest() {
//        this.sessionId = Dict.EMPTY;
//        this.name = Dict.EMPTY;
//        this.commandArguments = new String[0];
//        this.environmentVariables = new HashMap<>();
//        this.files = new HashMap<>();
//        this.zippedFiles = new HashMap<>();
//        this.options = new HashMap<>();
//        this.deepspeedConfigs = new HashMap<>();
//    }
//
//    public static StartDeepspeedContainerRequest fromStartContainersRequest(StartDeepspeedContainerRequest src) {
//        StartDeepspeedContainerRequest dst = new StartDeepspeedContainerRequest();
//        dst.sessionId = src.sessionId;
//        dst.name = src.name;
//        dst.commandArguments = src.commandArguments;
//        dst.environmentVariables = new HashMap<>(src.environmentVariables);
//        dst.files = new HashMap<>(src.files);
//        dst.zippedFiles = new HashMap<>(src.zippedFiles);
//        dst.options = new HashMap<>(src.options);
//        dst.deepspeedConfigs = new HashMap<>();
//        src.typedExtraConfigs.forEach((k,v)->{
//            dst.deepspeedConfigs.put(k, DeepspeedContainerConfig.deserialize(ByteString.copyFrom(v)));
//        });
//
//        return dst;
//    }
//
//    public static StartContainersRequest toStartContainersRequest(StartDeepspeedContainerRequest src) {
//        StartContainersRequest dst = new StartContainersRequest();
//        dst.sessionId = src.sessionId;
//        dst.name = src.name;
//        dst.jobType = JobProcessorTypes.deepSpeed.name();
//        dst.commandArguments = src.commandArguments;
//        dst.environmentVariables = new HashMap<>(src.environmentVariables);
//        dst.files = new HashMap<>(src.files);
//        dst.zippedFiles = new HashMap<>(src.zippedFiles);
//        dst.options = new HashMap<>(src.options);
//        dst.typedExtraConfigs = new HashMap<>();
//
//        for (Map.Entry<Long, DeepspeedContainerConfig> entry : src.deepspeedConfigs.entrySet()) {
//            Long key = entry.getKey();
//            DeepspeedContainerConfig value = entry.getValue();
//            dst.typedExtraConfigs.put(key, DeepspeedContainerConfig.serialize(value));
//        }
//
//        return dst;
//    }
//}
