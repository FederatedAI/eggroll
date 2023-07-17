package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class ErProcessorBatch_JAVA implements NetworkingRpcMessage_JAVA {
    private Long id;
    private String name;
    private List<ErProcessor_JAVA> processors;
    private String tag;

    public ErProcessorBatch_JAVA() {
        this.id = -1L;
        this.name = StringConstants.EMPTY();
        this.processors = new ArrayList<>();
        this.tag = StringConstants.EMPTY();
    }


    @Override
    public String toString() {//TODO List的输出
        return "<ErProcessorBatch(id=" + id + ", name=" + name +
                ", processors=" + processors.toString() + ", tag=" + tag +
                ") at " + Integer.toHexString(hashCode()) + ">";
    }

    public Meta.ProcessorBatch toProto() {
        Meta.ProcessorBatch.Builder builder = Meta.ProcessorBatch.newBuilder();
        builder.setId(this.getId())
                .setName(this.getName())
                .addAllProcessors(this.getProcessors().stream().map(ErProcessor_JAVA::toProto).collect(Collectors.toList()))
                .setTag(this.getTag());
        return builder.build();
    }
}