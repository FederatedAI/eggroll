package org.fedai.eggroll.core.pojo;


import org.fedai.eggroll.core.utils.JsonUtil;
import com.webank.eggroll.core.meta.Meta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ErStoreList implements RpcMessage {
    Logger log = LoggerFactory.getLogger(ErStoreList.class);
    private List<ErStore> stores;
    private Map<String, String> options;

    public List<ErStore> getStores() {
        return stores;
    }

    public void setStores(List<ErStore> stores) {
        this.stores = stores;
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public void setOptions(Map<String, String> options) {
        this.options = options;
    }

    public ErStoreList(List<ErStore> stores, Map<String, String> options) {
        this.stores = stores;
        this.options = options;
    }

    public ErStoreList() {
        this(new ArrayList<>(), new ConcurrentHashMap<>());
    }


    @Override
    public String toString() {
        return "ErStoreList{" +
                "stores=" + JsonUtil.object2Json(stores) +
                ", options=" + options +
                '}';
    }

    public Meta.StoreList toProto() {
        Meta.StoreList.Builder builder = Meta.StoreList.newBuilder();
        if (this.stores != null) {
            for (ErStore store : this.stores) {
                builder.addStores(store.toProto());
            }
        }
        return builder.build();
    }

    public static ErStoreList fromProto(Meta.StoreList storeList) {
        ErStoreList erStoreList = new ErStoreList();
        erStoreList.deserialize(storeList.toByteArray());
        return erStoreList;
    }

    @Override
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    @Override
    public void deserialize(byte[] data) {
        try {
            Meta.StoreList storeList = Meta.StoreList.parseFrom(data);
            this.stores = storeList.getStoresList().stream().map(ErStore::fromProto).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("deserialize error : ", e);
        }
    }
}
