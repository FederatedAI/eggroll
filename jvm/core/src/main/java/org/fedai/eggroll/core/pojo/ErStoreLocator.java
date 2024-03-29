package org.fedai.eggroll.core.pojo;

import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.constant.StringConstants;
import com.webank.eggroll.core.meta.Meta;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


@Data
public class ErStoreLocator implements RpcMessage, Cloneable {
    Logger log = LoggerFactory.getLogger(ErStoreLocator.class);

    private Long id;
    private String storeType;
    private String namespace;
    private String name;
    private String path;
    private Integer totalPartitions;
    private Integer keySerdesType;
    private Integer valueSerdesType;
    private Integer partitionerType;

    public ErStoreLocator() {

    }

    public ErStoreLocator(long id, String storeType, String namespace, String name, String path, int totalPartitions, int keySerdesType, int valueSerdesType, int partitionerType) {
        this.id = id;
        this.storeType = storeType;
        this.namespace = namespace;
        this.name = name;
        this.path = path;
        this.totalPartitions = totalPartitions;
        this.keySerdesType = keySerdesType;
        this.valueSerdesType = valueSerdesType;
        this.partitionerType = partitionerType;
    }

    public ErStoreLocator(String storeType, String namespace, String name) {
        this(-1L, storeType, namespace, name, "", 0, 0, 0, 0);
    }

    public String toPath(String delim) {
        if (!StringUtils.isBlank(path)) {
            return path;
        } else {
            return String.join(delim, storeType, namespace, name);
        }
    }

    public ErStoreLocator fork() {
        return fork(Dict.EMPTY, StringConstants.UNDERLINE);
    }

    public ErStoreLocator fork(String postfix, String delimiter) {
        int delimiterPos = StringUtils.lastIndexOf(this.name, delimiter, StringUtils.lastIndexOf(this.name, delimiter) - 1);

        String newPostfix = StringUtils.isBlank(postfix) ? String.join(delimiter, String.valueOf(System.currentTimeMillis()), UUID.randomUUID().toString()) : postfix;
        String newName;
        if (delimiterPos > 0) {
            newName = StringUtils.substring(this.name, 0, delimiterPos) + delimiter + newPostfix;
        } else {
            newName = this.name + delimiter + newPostfix;
        }

        return new ErStoreLocator(this.id, this.storeType, this.namespace, newName, this.path, this.totalPartitions, this.keySerdesType, this.valueSerdesType, this.partitionerType);
    }

    public Meta.StoreLocator toProto() {
        Meta.StoreLocator.Builder builder = Meta.StoreLocator.newBuilder();
        builder.setId(this.id)
                .setStoreType(this.storeType)
                .setNamespace(this.namespace)
                .setName(this.name)
                .setPath(this.path)
                .setTotalPartitions(this.totalPartitions)
                .setKeySerdesType(this.keySerdesType)
                .setValueSerdesType(this.valueSerdesType)
                .setPartitionerType(this.partitionerType);
        return builder.build();
    }

    public static ErStoreLocator fromProto(Meta.StoreLocator storeLocator) {
        ErStoreLocator erStoreLocator = new ErStoreLocator();
        erStoreLocator.deserialize(storeLocator.toByteArray());

        return erStoreLocator;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.StoreLocator storeLocator = Meta.StoreLocator.parseFrom(data);
            this.id = storeLocator.getId();
            this.storeType = storeLocator.getStoreType();
            this.namespace = storeLocator.getNamespace();
            this.name = storeLocator.getName();
            this.path = storeLocator.getPath();
            this.totalPartitions = storeLocator.getTotalPartitions();
            this.keySerdesType = storeLocator.getKeySerdesType();
            this.valueSerdesType = storeLocator.getValueSerdesType();
            this.partitionerType = storeLocator.getPartitionerType();
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public String buildKey() {
        return (this.namespace == null ? "" : this.namespace) + "_" +
                (this.name == null ? "" : this.name) + "_" +
                (this.storeType == null ? "" : this.storeType);
    }
}