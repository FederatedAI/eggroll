package com.eggroll.core.pojo;

import com.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.Deepspeed;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@Data
public class QueryJobResponse implements RpcMessage {
    Logger log = LoggerFactory.getLogger(QueryJobResponse.class);
    private String sessionId;
    private String jobType;
    private String status;
    private List<ErProcessor> processors = new ArrayList<>();

    @Override
    public byte[] serialize() {
        Deepspeed.QueryJobResponse.Builder builder = Deepspeed.QueryJobResponse.newBuilder();
        if (this.sessionId != null) {
            builder.setSessionId(this.sessionId);
        }
        if (this.jobType != null) {
            builder.setJobType(this.jobType);
        }
        if (this.status != null) {
            builder.setStatus(this.status);
        }
        if (this.processors != null) {
            for (ErProcessor processor : this.processors) {
                builder.addProcessors(processor.toProto());
            }
        }
        return builder.build().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Deepspeed.QueryJobResponse response = Deepspeed.QueryJobResponse.parseFrom(data);
            this.sessionId = response.getSessionId();
            this.setJobType(response.getJobType());
            this.setStatus(response.getStatus());
            if (response.getProcessorsList() != null) {
                for (Meta.Processor processor : response.getProcessorsList()) {
                    this.processors.add(ErProcessor.fromProto(processor));
                }
            }
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
