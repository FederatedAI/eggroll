package org.fedai.eggroll.core.pojo;

import com.google.protobuf.ByteString;
import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.fedai.eggroll.core.config.Dict;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class StartContainersRequest implements RpcMessage {
    Logger log = LoggerFactory.getLogger(StartContainersRequest.class);
    private String sessionId;
    private String name;
    private String jobType;
    private List<String> commandArguments;
    private Map<String, String> environmentVariables;
    private Map<String, byte[]> files;
    private Map<String, byte[]> zippedFiles;
    private Map<Long, byte[]> typedExtraConfigs;
    private Map<String, String> options;

    public StartContainersRequest() {
        this.sessionId = Dict.EMPTY;
        this.name = Dict.EMPTY;
        this.jobType = null;
        this.commandArguments = new ArrayList<>();
        this.environmentVariables = new HashMap<>();
        this.files = new HashMap<>();
        this.zippedFiles = new HashMap<>();
        this.typedExtraConfigs = new HashMap<>();
        this.options = new HashMap<>();
    }

    @Override
    public byte[] serialize() {
        Containers.StartContainersRequest.Builder builder = Containers.StartContainersRequest.newBuilder()
                .setSessionId(this.sessionId)
                .setName(this.name)
                .addAllCommandArguments(this.commandArguments)
                .putAllEnvironmentVariables(this.environmentVariables)
                .putAllFiles(convertToByteStringMap(this.files))
                .putAllZippedFiles(convertToByteStringMap(this.zippedFiles))
                .putAllTypedExtraConfigs(convertToByteStringMapWithLongKeys(this.typedExtraConfigs))
                .putAllOptions(this.options);

        builder.setJobType(this.jobType);

        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Containers.StartContainersRequest src = Containers.StartContainersRequest.parseFrom(data);
            this.sessionId = src.getSessionId();
            this.name = src.getName();
            this.jobType = src.getJobType();

            this.commandArguments = src.getCommandArgumentsList();
            this.environmentVariables = new HashMap<>(src.getEnvironmentVariablesMap());
            this.files = new HashMap<>();
            for (Map.Entry<String, ByteString> entry : src.getFilesMap().entrySet()) {
                this.files.put(entry.getKey(), entry.getValue().toByteArray());
            }

            this.zippedFiles = new HashMap<>();
            for (Map.Entry<String, ByteString> entry : src.getZippedFilesMap().entrySet()) {
                this.zippedFiles.put(entry.getKey(), entry.getValue().toByteArray());
            }

            this.typedExtraConfigs = new HashMap<>();
            src.getTypedExtraConfigsMap().forEach((key, value) -> this.typedExtraConfigs.put(key, value.toByteArray()));
            this.options = new HashMap<>(src.getOptionsMap());
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
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