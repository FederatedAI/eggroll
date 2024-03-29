package org.fedai.eggroll.core.deepspeed.store;

import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.fedai.eggroll.core.pojo.RpcMessage;

@Data
public class RendezvousStoreSetResponse implements RpcMessage {

    @Override
    public byte[] serialize() {
        Deepspeed.StoreSetResponse.Builder builder = Deepspeed.StoreSetResponse.newBuilder();
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
    }
}