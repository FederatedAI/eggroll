package com.eggroll.core.pojo;


import com.google.protobuf.InvalidProtocolBufferException;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Data
public class ErPartition implements RpcMessage,Cloneable {
    Logger log = LoggerFactory.getLogger(ErPartition.class);
    private int id =-1;
    private ErStoreLocator storeLocator;
    private ErProcessor processor;
    private int rankInNode = -1;


    public ErPartition(){

    }

    public ErPartition(int id, ErStoreLocator storeLocator, ErProcessor processor, int rankInNode) {
        this.id = id;
        this.storeLocator = storeLocator;
        this.processor = processor;
        this.rankInNode = rankInNode;
    }

    public ErPartition(int id) {
        this(id, null, null, -1);
    }

    public String toPath(String delim) {
        String storeLocatorPath = storeLocator != null ? storeLocator.toPath(delim) : "";
        return String.join(delim, storeLocatorPath, String.valueOf(id));
    }

    public Meta.Partition toProto(){
        Meta.Partition.Builder builder = Meta.Partition.newBuilder();
        builder.setId(this.id)
                .setStoreLocator(this.storeLocator.toProto())
                .setProcessor(this.processor.toProto())
                .setRankInNode(this.rankInNode);
        return builder.build();
    }

    public static ErPartition fromProto(Meta.Partition partition){
        ErPartition erPartition = new ErPartition();
        erPartition.deserialize(partition.toByteArray());
        return erPartition;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.Partition partition = Meta.Partition.parseFrom(data);
            this.id = partition.getId();
            this.storeLocator = ErStoreLocator.fromProto(partition.getStoreLocator());
            this.processor = ErProcessor.fromProto(partition.getProcessor());
            this.rankInNode = partition.getRankInNode();
        } catch (InvalidProtocolBufferException e) {
            log.error("deserialize error : ",e);
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
