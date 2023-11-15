package com.eggroll.core.pojo;

import com.eggroll.core.constant.StringConstants;
import com.google.protobuf.ByteString;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class SubmitJobRequest implements RpcMessage {
    Logger log = LoggerFactory.getLogger(SubmitJobRequest.class);

    private String sessionId = StringConstants.EMPTY;
    private String name = StringConstants.EMPTY;
    private String jobType = StringConstants.EMPTY;
    private Integer worldSize = 0;
    private List<String> commandArguments = new ArrayList<>();
    private Map<String, String> environmentVariables = new HashMap<>();
    private Map<String, byte[]> files;
    private Map<String, byte[]> zippedFiles;
    private ResourceOptions resourceOptions;
    private Map<String, String> options = new HashMap<>();

    public Deepspeed.SubmitJobRequest toProto() {
        Deepspeed.SubmitJobRequest.Builder builder = Deepspeed.SubmitJobRequest.newBuilder();
        builder.setSessionId(this.sessionId)
                .setName(this.name)
                .setJobType(this.jobType)
                .setWorldSize(this.worldSize)
                .addAllCommandArguments(this.commandArguments)
                .putAllEnvironmentVariables(this.environmentVariables)
                .setResourceOptions(this.resourceOptions.toProto())
                .putAllOptions(this.options);
        if (this.files != null) {
            this.files.forEach((k, v) -> {
                builder.putFiles(k, ByteString.copyFrom(v));
            });
        }

        if (this.zippedFiles != null) {
            this.zippedFiles.forEach((k, v) -> {
                builder.putZippedFiles(k, ByteString.copyFrom(v));
            });
        }
        return builder.build();
    }

    public static SubmitJobRequest fromProto(Deepspeed.SubmitJobRequest submitJobRequest) {
        SubmitJobRequest result = new SubmitJobRequest();
        result.deserialize(submitJobRequest.toByteArray());
        return result;
    }


    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.SubmitJobRequest submitJobRequest = Deepspeed.SubmitJobRequest.parseFrom(data);
            this.sessionId = submitJobRequest.getSessionId();
            this.name = submitJobRequest.getName();
            this.jobType = submitJobRequest.getJobType();
            this.worldSize = submitJobRequest.getWorldSize();
            this.commandArguments = submitJobRequest.getCommandArgumentsList();
            this.environmentVariables = submitJobRequest.getEnvironmentVariablesMap();
            this.options = submitJobRequest.getOptionsMap();
            if (submitJobRequest.getFilesMap() != null) {
                this.files = new HashMap<>();
                submitJobRequest.getFilesMap().forEach((k, v) -> {
                    this.files.put(k, v.toByteArray());
                });
            }
            if (submitJobRequest.getZippedFilesMap() != null) {
                this.zippedFiles = new HashMap<>();
                submitJobRequest.getZippedFilesMap().forEach((k, v) -> {
                    this.zippedFiles.put(k, v.toByteArray());
                });
            }
            this.resourceOptions = ResourceOptions.fromProto(submitJobRequest.getResourceOptions());
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }

    }
}
