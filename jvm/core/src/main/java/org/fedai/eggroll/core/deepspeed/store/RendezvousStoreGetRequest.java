package org.fedai.eggroll.core.deepspeed.store;

import org.fedai.eggroll.core.pojo.RpcMessage;
import org.fedai.eggroll.core.utils.JsonUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class RendezvousStoreGetRequest implements RpcMessage {
    Logger log = LoggerFactory.getLogger(RendezvousStoreGetRequest.class);

    private String prefix;
    private byte[] key;
    private int timeout;

    @Override
    public byte[] serialize() {

        log.info("=>>>>>>>>>>>> serialize's timeout = {} ",timeout);
        Deepspeed.StoreGetRequest.Builder builder = Deepspeed.StoreGetRequest.newBuilder()
                .setPrefix(this.getPrefix())
                .setKey(ByteString.copyFrom(JsonUtil.convertToByteArray(this.getKey())))
                .setTimeout(com.google.protobuf.Duration.newBuilder()
                        .setSeconds(this.getTimeout()/1000)
                        .setNanos((this.getTimeout()%1000)*1000000)
                        .build());
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.StoreGetRequest proto = Deepspeed.StoreGetRequest.parseFrom(data);
            this.prefix = proto.getPrefix();
            this.key = proto.getKey().toByteArray();
            this.timeout = (int)(proto.getTimeout().getSeconds()*1000 + proto.getTimeout().getNanos()/1000000);
            log.info("=>>>>>>>>>>>> deserialize's timeout = {} ",this.timeout);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

}