package org.fedai.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Data
public class MetaInfoResponse implements RpcMessage{

    Logger log = LoggerFactory.getLogger(MetaInfoResponse.class);

    private Map<String,String> metaMap;

    @Override
    public byte[] serialize() {
        Meta.MetaInfoResponse.Builder builder = Meta.MetaInfoResponse.newBuilder();
        builder.putAllMetaMap(this.metaMap);
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.MetaInfoResponse metaInfoResponse = Meta.MetaInfoResponse.parseFrom(data);
            this.metaMap = metaInfoResponse.getMetaMapMap();
        }catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
