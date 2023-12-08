package org.fedai.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class MetaInfoRequest implements RpcMessage {

    Logger log = LoggerFactory.getLogger(MetaInfoRequest.class);

    private String key = "test";

    @Override
    public byte[] serialize() {
        Meta.MetaInfoRequest.Builder builder = Meta.MetaInfoRequest.newBuilder();
        builder.setKey(this.key);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.MetaInfoRequest metaInfoRequest = Meta.MetaInfoRequest.parseFrom(data);
            this.key = metaInfoRequest.getKey();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
