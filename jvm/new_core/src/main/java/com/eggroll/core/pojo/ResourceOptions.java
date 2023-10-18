package com.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class ResourceOptions implements RpcMessage {
    Logger log = LoggerFactory.getLogger(ResourceOptions.class);
    private Integer timeoutSeconds = 0;
    private String resourceExhaustedStrategy;

    public Deepspeed.ResourceOptions toProto() {
        Deepspeed.ResourceOptions.Builder builder = Deepspeed.ResourceOptions.newBuilder();
        builder.setTimeoutSeconds(this.timeoutSeconds);
        if(this.resourceExhaustedStrategy!=null){
            builder.setResourceExhaustedStrategy(this.resourceExhaustedStrategy);
        }

        return builder.build();
    }

    public static ResourceOptions fromProto(Deepspeed.ResourceOptions resourceOptions) {
        ResourceOptions result = new ResourceOptions();
        result.deserialize(resourceOptions.toByteArray());
        return result;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.ResourceOptions resourceOptions = Deepspeed.ResourceOptions.parseFrom(data);
            this.timeoutSeconds = resourceOptions.getTimeoutSeconds();
            this.resourceExhaustedStrategy = resourceOptions.getResourceExhaustedStrategy();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }

    }
}
