package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Containers;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Data
public class DownloadContainersRequest implements RpcMessage {

    Logger log = LoggerFactory.getLogger(ContainerContent.class);

    private String sessionId;
    private String compressMethod;
    private List<Integer> ranks;
    private int compressLevel;
    private Containers.ContentType contentType;

    public DownloadContainersRequest(String sessionId, String compressMethod, List<Integer> ranks, int compressLevel, Containers.ContentType contentType) {
        this.sessionId = sessionId;
        this.compressMethod = compressMethod;
        this.ranks = ranks;
        this.compressLevel = compressLevel;
        this.contentType = contentType;
    }

    public Containers.DownloadContainersRequest toProto() {
        Containers.DownloadContainersRequest.Builder builder = Containers.DownloadContainersRequest.newBuilder();
        if (this.sessionId != null) {
            builder.setSessionId(this.sessionId);
        }
        if (this.compressMethod != null) {
            builder.setCompressMethod(this.compressMethod);
        }
        builder.setCompressLevel(this.compressLevel);
        if (this.contentType != null) {
            builder.setContentType(this.contentType);
        }
        return builder.build();
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Containers.DownloadContainersRequest src = Containers.DownloadContainersRequest.parseFrom(data);
            this.sessionId = src.getSessionId();
            this.compressMethod = src.getCompressMethod();
            this.compressLevel = src.getCompressLevel();
            this.contentType = src.getContentType();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
