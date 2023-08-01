package com.eggroll.core.pojo;

import com.eggroll.core.config.Dict;
import com.eggroll.core.constant.StringConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;


public class ErStoreLocator implements MetaRpcMessage {
    private Long id;
    private String storeType;
    private String namespace;
    private String name;
    private String path;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getStoreType() {
        return storeType;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getTotalPartitions() {
        return totalPartitions;
    }

    public void setTotalPartitions(Integer totalPartitions) {
        this.totalPartitions = totalPartitions;
    }

    public String getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(String partitioner) {
        this.partitioner = partitioner;
    }

    public String getSerdes() {
        return serdes;
    }

    public void setSerdes(String serdes) {
        this.serdes = serdes;
    }

    private Integer totalPartitions;
    private String partitioner;
    private String serdes;

    public ErStoreLocator(long id, String storeType, String namespace, String name, String path, int totalPartitions, String partitioner, String serdes) {
        this.id = id;
        this.storeType = storeType;
        this.namespace = namespace;
        this.name = name;
        this.path = path;
        this.totalPartitions = totalPartitions;
        this.partitioner = partitioner;
        this.serdes = serdes;
    }

    public ErStoreLocator(String storeType, String namespace, String name) {
        this(-1L, storeType, namespace, name, "", 0, "", "");
    }

    public String toPath(String delim) {
        if (!StringUtils.isBlank(path)) {
            return path;
        } else {
            return String.join(delim, storeType, namespace, name);
        }
    }

    public ErStoreLocator fork(){
        return fork(Dict.EMPTY,StringConstants.UNDERLINE);
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

        return new ErStoreLocator(this.id, this.storeType, this.namespace, newName, this.path, this.totalPartitions, this.partitioner, this.serdes);
    }


}