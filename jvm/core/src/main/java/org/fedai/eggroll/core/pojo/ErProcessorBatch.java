package org.fedai.eggroll.core.pojo;


import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.fedai.eggroll.core.constant.StringConstants;
import org.fedai.eggroll.core.utils.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class ErProcessorBatch implements RpcMessage {
    Logger log = LoggerFactory.getLogger(ErProcessorBatch.class);
    private long id;
    private String name;
    private List<ErProcessor> processors;
    private String tag;


    public ErProcessorBatch() {
        this.id = -1;
        this.name = StringConstants.EMPTY;
        this.processors = new ArrayList<>();
        this.tag = StringConstants.EMPTY;
    }

    public ErProcessorBatch(long id, String name, List<ErProcessor> processors, String tag) {
        this.id = id;
        this.name = name;
        this.processors = processors;
        this.tag = tag;
    }

    @Override
    public String toString() {
        return "<ErProcessorBatch(id=" + id + ", name=" + name +
                ", processors=" + JsonUtil.object2Json(processors) + ", tag=" + tag +
                ") at " + Integer.toHexString(hashCode()) + ">";
    }

    public Meta.ProcessorBatch toProto() {
        Meta.ProcessorBatch.Builder builder = Meta.ProcessorBatch.newBuilder();
        builder.setId(this.getId())
                .setName(this.getName())
                .addAllProcessors(this.getProcessors().stream().map(ErProcessor::toProto).collect(Collectors.toList()))
                .setTag(this.getTag());
        return builder.build();
    }

    public static ErProcessorBatch fromProto(Meta.ProcessorBatch processorBatch) {
        ErProcessorBatch erProcessorBatch = new ErProcessorBatch();
        erProcessorBatch.deserialize(processorBatch.toByteArray());
        return erProcessorBatch;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.ProcessorBatch processorBatch = Meta.ProcessorBatch.parseFrom(data);
            this.id = processorBatch.getId();
            this.name = processorBatch.getName();
            this.processors = processorBatch.getProcessorsList().stream().map(ErProcessor::fromProto).collect(Collectors.toList());
            this.tag = processorBatch.getTag();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
