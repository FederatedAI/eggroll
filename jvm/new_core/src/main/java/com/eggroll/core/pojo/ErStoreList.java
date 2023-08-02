package com.eggroll.core.pojo;


import com.eggroll.core.utils.JsonUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ErStoreList implements MetaRpcMessage {
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
}
