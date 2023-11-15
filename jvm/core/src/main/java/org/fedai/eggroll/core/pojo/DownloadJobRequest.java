package org.fedai.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Containers;
import com.webank.eggroll.core.meta.Deepspeed;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Data
public class DownloadJobRequest implements RpcMessage {

    Logger log = LoggerFactory.getLogger(DownloadJobRequest.class);

    private String sessionId;
    List<Integer> ranks;
    private String compressMethod;
    private Integer compressLevel = 1;
    Containers.ContentType contentType;


    public Deepspeed.DownloadJobRequest toProto() {
        Deepspeed.DownloadJobRequest.Builder builder = Deepspeed.DownloadJobRequest.newBuilder();
        builder.setSessionId(this.sessionId);
        builder.setCompressMethod(this.compressMethod);
        builder.addAllRanks(this.ranks);
        builder.setCompressLevel(this.compressLevel);
        builder.setContentType(this.contentType);

        return builder.build();
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.DownloadJobRequest downloadJobRequest = Deepspeed.DownloadJobRequest.parseFrom(data);
            this.sessionId = downloadJobRequest.getSessionId();
            this.ranks = downloadJobRequest.getRanksList();
            this.compressMethod = downloadJobRequest.getCompressMethod();
            this.compressLevel = downloadJobRequest.getCompressLevel();
            this.contentType = downloadJobRequest.getContentType();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
