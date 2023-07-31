package com.webank.eggroll.clustermanager.entity.scala;

import com.webank.eggroll.core.util.JsonUtil;
import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class ErStoreList_JAVA implements MetaRpcMessage_JAVA {
    private List<ErStore_JAVA> stores;
    private Map<String, String> options;

    public ErStoreList_JAVA(List<ErStore_JAVA> stores, Map<String, String> options) {
        this.stores = stores;
        this.options = options;
    }

    public ErStoreList_JAVA() {
        this(new ArrayList<>(), new ConcurrentHashMap<>());
    }


    @Override
    public String toString() {
        return "ErStoreList{" +
                "stores=" + JsonUtil.object2Json(stores) +
                ", options=" + options +
                '}';
    }
}