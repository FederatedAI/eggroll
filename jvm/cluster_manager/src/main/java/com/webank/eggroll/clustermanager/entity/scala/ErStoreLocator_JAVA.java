package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.constant.StringConstants;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

@Data
public class ErStoreLocator_JAVA implements MetaRpcMessage_JAVA {
    private Long id;
    private String storeType;
    private String namespace;
    private String name;
    private String path;
    private Integer totalPartitions;
    private String partitioner;
    private String serdes;

    public ErStoreLocator_JAVA(long id, String storeType, String namespace, String name, String path, int totalPartitions, String partitioner, String serdes) {
        this.id = id;
        this.storeType = storeType;
        this.namespace = namespace;
        this.name = name;
        this.path = path;
        this.totalPartitions = totalPartitions;
        this.partitioner = partitioner;
        this.serdes = serdes;
    }

    public ErStoreLocator_JAVA(String storeType, String namespace, String name) {
        this(-1L, storeType, namespace, name, "", 0, "", "");
    }

    public String toPath(String delim) {
        if (!StringUtils.isBlank(path)) {
            return path;
        } else {
            return String.join(delim, storeType, namespace, name);
        }
    }

    public ErStoreLocator_JAVA fork(){
        return fork(StringConstants.EMPTY(),StringConstants.UNDERLINE());
    }

    public ErStoreLocator_JAVA fork(String postfix, String delimiter) {
        int delimiterPos = StringUtils.lastIndexOf(this.name, delimiter, StringUtils.lastIndexOf(this.name, delimiter) - 1);

        String newPostfix = StringUtils.isBlank(postfix) ? String.join(delimiter, String.valueOf(System.currentTimeMillis()), UUID.randomUUID().toString()) : postfix;
        String newName;
        if (delimiterPos > 0) {
            newName = StringUtils.substring(this.name, 0, delimiterPos) + delimiter + newPostfix;
        } else {
            newName = this.name + delimiter + newPostfix;
        }

        return new ErStoreLocator_JAVA(this.id, this.storeType, this.namespace, newName, this.path, this.totalPartitions, this.partitioner, this.serdes);
    }


}