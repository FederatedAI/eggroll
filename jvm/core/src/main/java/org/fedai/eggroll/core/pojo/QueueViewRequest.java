package org.fedai.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueViewRequest implements RpcMessage{
    Logger log = LoggerFactory.getLogger(QueueViewRequest.class);

    private String key = "test";

    @Override
    public byte[] serialize() {
        Meta.QueueViewRequest.Builder builder = Meta.QueueViewRequest.newBuilder();
        builder.setKey(this.key);
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.QueueViewRequest metaInfoRequest = Meta.QueueViewRequest.parseFrom(data);
            this.key = metaInfoRequest.getKey();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
