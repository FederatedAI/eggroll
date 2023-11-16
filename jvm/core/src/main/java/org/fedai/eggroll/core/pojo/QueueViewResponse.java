package org.fedai.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class QueueViewResponse implements RpcMessage{

    Logger log = LoggerFactory.getLogger(QueueViewResponse.class);
    private Integer queueSize;


    @Override
    public byte[] serialize() {
        Meta.QueueViewResponse.Builder builder = Meta.QueueViewResponse.newBuilder();
        builder.setQueueSize(this.queueSize);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.QueueViewResponse queueViewResponse = Meta.QueueViewResponse.parseFrom(data);
            this.queueSize = queueViewResponse.getQueueSize();
        }catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
