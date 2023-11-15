package org.fedai.eggroll.core.pojo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.DeepspeedDownload;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

@Data
public class PrepareJobDownloadRequest implements RpcMessage {

    String sessionId;
    List<Integer> ranks;
    String compressMethod;
    Integer compressLevel = 1;
    String contentType;

    @Override
    public byte[] serialize() {
        DeepspeedDownload.PrepareDownloadRequest.Builder builder = DeepspeedDownload.PrepareDownloadRequest.newBuilder();
        builder.setSessionId(sessionId);
        if (ranks != null) {
            builder.addAllRanks(ranks);
        }
        if (StringUtils.isNotEmpty(compressMethod)) {
            builder.setCompressMethod(compressMethod);
        }
        builder.setCompressLevel(compressLevel);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            DeepspeedDownload.PrepareDownloadRequest request = DeepspeedDownload.PrepareDownloadRequest.parseFrom(data);
            this.sessionId = request.getSessionId();
            this.ranks = request.getRanksList();
            this.compressMethod = request.getCompressMethod();
            if (request.getCompressLevel() > 0) {
                compressLevel = request.getCompressLevel();
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }
}
