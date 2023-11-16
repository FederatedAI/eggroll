package org.fedai.eggroll.core.pojo;

import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Data
public class ErStore implements RpcMessage, Cloneable {
    Logger log = LoggerFactory.getLogger(ErStore.class);
    private ErStoreLocator storeLocator;
    private List<ErPartition> partitions;
    private Map<String, String> options;

    public ErStore() {
        this.storeLocator = null;
        this.partitions = new ArrayList<>();
        this.options = new HashMap<>();
    }

    public ErStore(ErStoreLocator storeLocator, List<ErPartition> partitions, Map<String, String> options) {
        this.storeLocator = storeLocator;
        this.partitions = partitions;
        this.options = options;
    }

    public ErStore(ErStoreLocator storeLocator) {
        this(storeLocator, new ArrayList<>(), new ConcurrentHashMap<>());
    }

    public String toPath(String delim) {
        return storeLocator.toPath(delim);
    }

    public ErStore fork(ErStoreLocator storeLocator) {
        ErStoreLocator finalStoreLocator = storeLocator == null ? storeLocator.fork() : storeLocator;

        List<ErPartition> updatedPartitions = new ArrayList<>();
        for (int i = 0; i < partitions.size(); i++) {
            ErPartition partition = partitions.get(i);
            partition.setStoreLocator(finalStoreLocator);
        }
        return new ErStore(finalStoreLocator, updatedPartitions, options);
    }

    public ErStore fork(String postfix, String delimiter) {
        ErStoreLocator newStoreLocator = storeLocator.fork(postfix, delimiter);
        return fork(newStoreLocator);
    }

    public Meta.Store toProto() {
        Meta.Store.Builder builder = Meta.Store.newBuilder();
        builder.setStoreLocator(this.storeLocator.toProto())
                .addAllPartitions(this.partitions.stream().map(ErPartition::toProto).collect(Collectors.toList()))
                .putAllOptions(this.options);
        return builder.build();
    }

    public static ErStore fromProto(Meta.Store store) {
        ErStore erStore = new ErStore();
        erStore.deserialize(store.toByteArray());
        return erStore;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.Store store = Meta.Store.parseFrom(data);
            this.storeLocator = ErStoreLocator.fromProto(store.getStoreLocator());
            this.partitions = store.getPartitionsList().stream().map(ErPartition::fromProto).collect(Collectors.toList());
            this.options.putAll(store.getOptionsMap());
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        ErStore erStore = (ErStore) super.clone();
        if (this.storeLocator != null) {
            erStore.setStoreLocator(ObjectUtils.clone(this.storeLocator));
        }
        if (this.partitions != null) {
            List<ErPartition> cloneErPartitions = new ArrayList<>();
            for (ErPartition partition : this.partitions) {
                if (partition != null) {
                    cloneErPartitions.add(ObjectUtils.clone(partition));
                }
            }
            erStore.setPartitions(cloneErPartitions);
        }
        if (this.options != null) {
            Map<String, String> cloneOptions = new HashMap<>(this.options);
            erStore.setOptions(cloneOptions);
        }
        return erStore;
    }
}