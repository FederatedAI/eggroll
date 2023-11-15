package com.eggroll.core.pojo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.DeepspeedDownload;
import lombok.Data;

@Data
public class PrepareJobDownloadResponse implements RpcMessage {

    String sessionId;
    String content;

    @Override
    public byte[] serialize() {
        return DeepspeedDownload.PrepareDownloadResponse.newBuilder().setContent(content).setSessionId(sessionId).build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            DeepspeedDownload.PrepareDownloadResponse prepareDownloadResponse = DeepspeedDownload.PrepareDownloadResponse.parseFrom(data);
            this.sessionId = prepareDownloadResponse.getSessionId();
            this.content = prepareDownloadResponse.getContent();
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}
