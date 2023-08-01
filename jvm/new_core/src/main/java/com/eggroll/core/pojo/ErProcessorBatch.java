package com.eggroll.core.pojo;


import com.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.Meta;


import java.util.Arrays;
import java.util.stream.Collectors;


public class ErProcessorBatch {
        private long id;
        private String name;
        private ErProcessor[] processors;
        private String tag;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ErProcessor[] getProcessors() {
        return processors;
    }

    public void setProcessors(ErProcessor[] processors) {
        this.processors = processors;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public ErProcessorBatch() {
            this.id = -1;
            this.name = StringConstants.EMPTY;
            this.processors = new ErProcessor[0];
            this.tag = StringConstants.EMPTY;
        }

        public ErProcessorBatch(long id, String name, ErProcessor[] processors, String tag) {
            this.id = id;
            this.name = name;
            this.processors = processors;
            this.tag = tag;
        }

        @Override
        public String toString() {
            return "<ErProcessorBatch(id=" + id + ", name=" + name +
                    ", processors=" + Arrays.toString(processors) + ", tag=" + tag +
                    ") at " + Integer.toHexString(hashCode()) + ">";
        }

        public Meta.ProcessorBatch toProto() {
            Meta.ProcessorBatch.Builder builder = Meta.ProcessorBatch.newBuilder();
            builder.setId(this.getId())
                    .setName(this.getName())
                    .addAllProcessors(Arrays.stream(this.getProcessors()).map(ErProcessor::toProto).collect(Collectors.toList()))
                    .setTag(this.getTag());
            return builder.build();
        }
    }
