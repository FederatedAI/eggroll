package com.eggroll.core.deepspeed.store;

import com.eggroll.core.context.Context;
import com.google.inject.Singleton;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;

@Data
@Singleton
public class RendezvousStoreService {
    private ConcurrentHashMap<String, WaitableMapStore> stores;

    public RendezvousStoreService() {
        this.stores = new ConcurrentHashMap<>();
    }

    private WaitableMapStore getStore(String prefix) {
        return stores.computeIfAbsent(prefix, key -> new WaitableMapStore());
    }

    private boolean destroyStore(String prefix) {
        WaitableMapStore store = stores.remove(prefix);
        if (store != null) {
            store.destroy();
            return true;
        } else {
            return false;
        }
    }

    public RendezvousStoreDestroyResponse destroy(Context context, RendezvousStoreDestroyRequest rendezvousStoreDestroyRequest) {
        boolean success = destroyStore(rendezvousStoreDestroyRequest.getPrefix());
        RendezvousStoreDestroyResponse result = new RendezvousStoreDestroyResponse();
        result.setSuccess(success);
        return result;
    }

    public RendezvousStoreSetResponse set(Context context, RendezvousStoreSetRequest request) {
        WaitableMapStore store = getStore(request.getPrefix());
        store.set(new String(request.getKey()), request.getValue());
        return new RendezvousStoreSetResponse();
    }

    public RendezvousStoreGetResponse get(Context context, RendezvousStoreGetRequest request) throws InterruptedException {
        WaitableMapStore store = getStore(request.getPrefix());
        byte[] value = store.get(new String(request.getKey()), request.getTimeout());
        if (value != null) {
            return new RendezvousStoreGetResponse(value, false);
        } else {
            return new RendezvousStoreGetResponse(new byte[0], true);
        }
    }

    public RendezvousStoreAddResponse add(Context context, RendezvousStoreAddRequest request) {
        WaitableMapStore store = getStore(request.getPrefix());
        final long amount = store.add(new String(request.getKey()), request.getAmount());
        return new RendezvousStoreAddResponse(amount);
    }
}