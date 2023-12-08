package org.fedai.eggroll.core.pojo;

import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Deepspeed;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

@Data
public class SubmitJobResponse implements RpcMessage {
    Logger log = LoggerFactory.getLogger(SubmitJobResponse.class);
    private String sessionId;
    private List<ErProcessor> processors;

    @Override
    public byte[] serialize() {
        Deepspeed.SubmitJobResponse.Builder builder = Deepspeed.SubmitJobResponse.newBuilder()
                .setSessionId(this.sessionId)
                .addAllProcessors(this.processors.stream().map(ErProcessor::toProto).collect(Collectors.toList()));
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.SubmitJobResponse proto = Deepspeed.SubmitJobResponse.parseFrom(data);
            this.processors = proto.getProcessorsList().stream().map(ErProcessor::fromProto).collect(Collectors.toList());
            this.sessionId = proto.getSessionId();
        } catch (InvalidProtocolBufferException e) {
            log.error("deserialize error : ", e);
        }
    }

}
