package com.eggroll.core.pojo;

import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.StringConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Containers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class StartContainersRequest {
    public String sessionId;
    public String name;
    public String jobType;
    public String[] commandArguments;
    public Map<String, String> environmentVariables;
    public Map<String, byte[]> files;
    public Map<String, byte[]> zippedFiles;
    public Map<Long, byte[]> typedExtraConfigs;
    public Map<String, String> options;

    public StartContainersRequest() {
        this.sessionId = Dict.EMPTY;
        this.name = Dict.EMPTY;
        this.jobType = null;
        this.commandArguments = new String[0];
        this.environmentVariables = new HashMap<>();
        this.files = new HashMap<>();
        this.zippedFiles = new HashMap<>();
        this.typedExtraConfigs = new HashMap<>();
        this.options = new HashMap<>();
    }

    public static StartContainersRequest deserialize(ByteString byteString) throws InvalidProtocolBufferException {
        Containers.StartContainersRequest src = Containers.StartContainersRequest.parseFrom(byteString);
        StartContainersRequest dst = new StartContainersRequest();
        dst.sessionId = src.getSessionId();
        dst.name = src.getName();
        dst.jobType = src.getJobType();

        dst.commandArguments = src.getCommandArgumentsList().toArray(new String[0]);
        dst.environmentVariables = new HashMap<>(src.getEnvironmentVariablesMap());
        dst.files = new HashMap<>();
        for (Map.Entry<String, ByteString> entry : src.getFilesMap().entrySet()) {
            dst.files.put(entry.getKey(), entry.getValue().toByteArray());
        }

        dst.zippedFiles = new HashMap<>();
        for (Map.Entry<String, ByteString> entry : src.getZippedFilesMap().entrySet()) {
            dst.zippedFiles.put(entry.getKey(), entry.getValue().toByteArray());
        }

        dst.typedExtraConfigs = new HashMap<>();
        src.getTypedExtraConfigsMap().forEach((key, value) -> {
            dst.typedExtraConfigs.put(key, value.toByteArray());
        });
        dst.options = new HashMap<>(src.getOptionsMap());
        return dst;
    }

    public static ByteString serialize(StartContainersRequest src) {
        Containers.StartContainersRequest.Builder builder = Containers.StartContainersRequest.newBuilder()
                .setSessionId(src.sessionId)
                .setName(src.name)
                .addAllCommandArguments(Arrays.asList(src.commandArguments))
                .putAllEnvironmentVariables(src.environmentVariables)
                .putAllFiles(convertToByteStringMap(src.files))
                .putAllZippedFiles(convertToByteStringMap(src.zippedFiles))
                .putAllTypedExtraConfigs(convertToByteStringMapWithLongKeys(src.typedExtraConfigs))
                .putAllOptions(src.options);

        builder.setJobType(src.jobType);

        return builder.build().toByteString();
    }

    private static Map<String, ByteString> convertToByteStringMap(Map<String, byte[]> map) {
        Map<String, ByteString> byteStringMap = new HashMap<>();
        for (Map.Entry<String, byte[]> entry : map.entrySet()) {
            byteStringMap.put(entry.getKey(), ByteString.copyFrom(entry.getValue()));
        }
        return byteStringMap;
    }

    private static Map<Long, ByteString> convertToByteStringMapWithLongKeys(Map<Long, byte[]> map) {
        Map<Long, ByteString> byteStringMap = new HashMap<>();
        for (Map.Entry<Long, byte[]> entry : map.entrySet()) {
            byteStringMap.put(entry.getKey(), ByteString.copyFrom(entry.getValue()));
        }
        return byteStringMap;
    }
}