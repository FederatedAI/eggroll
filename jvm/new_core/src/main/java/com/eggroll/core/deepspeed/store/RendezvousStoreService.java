package com.eggroll.core.deepspeed.store;

import com.eggroll.core.context.Context;
import com.google.inject.Singleton;
import lombok.Data;

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Singleton
public class RendezvousStoreService<K> {
    private ConcurrentHashMap<String, WaitableMapStore<K>> stores;

    public RendezvousStoreService() {
        this.stores = new ConcurrentHashMap<>();
    }

    private WaitableMapStore<K> getStore(String prefix) {
        System.out.println("getStore: " + prefix);
        WaitableMapStore<K> store = stores.computeIfAbsent(prefix, key -> new WaitableMapStore<>());
        System.out.println("getStore: " + prefix + " done, store: " + store);
        return store;
    }

    private boolean destroyStore(String prefix) {
        WaitableMapStore<K> store = stores.remove(prefix);
        if (store != null) {
            store.destroy();
            return true;
        } else {
            return false;
        }
    }

    public RendezvousStoreDestroyResponse destroy(Context context , RendezvousStoreDestroyRequest rendezvousStoreDestroyRequest) {
        System.out.println("destroy: " + rendezvousStoreDestroyRequest);
        boolean success = destroyStore(rendezvousStoreDestroyRequest.getPrefix());
        System.out.println("destroy: " + rendezvousStoreDestroyRequest + " done, success: " + success);
        return new RendezvousStoreDestroyResponse(success);
    }

    public RendezvousStoreSetResponse set(Context context ,RendezvousStoreSetRequest<K> request) {
        WaitableMapStore<K> store = getStore(request.getPrefix());
        System.out.println("set: " + request + " to store " + store);
        store.set(request.getKey(), request.getValue());
        System.out.println("set: " + request + " done");
        return new RendezvousStoreSetResponse();
    }

    public RendezvousStoreGetResponse<K> get(Context context ,RendezvousStoreGetRequest<K> request) throws InterruptedException {
        System.out.println("get: " + request + " to store " + stores);
        WaitableMapStore<K> store = getStore(request.getPrefix());
        Vector value = store.get(request.getKey(), request.getTimeout());
        if (value != null) {
            System.out.println("get: " + request + " done");
            return new RendezvousStoreGetResponse(value, false);
        } else {
            System.out.println("get: " + request + " timeout");
            return new RendezvousStoreGetResponse("", true);
        }
    }

    public RendezvousStoreAddResponse add(Context context ,RendezvousStoreAddRequest<K> request) {
        System.out.println("add: " + request + " to store " + stores);
        WaitableMapStore<K> store = getStore(request.getPrefix());
        final long amount = store.add(request.getKey(), request.getAmount());
        System.out.println("add: " + request + " done");
        return new RendezvousStoreAddResponse(amount);
    }
}