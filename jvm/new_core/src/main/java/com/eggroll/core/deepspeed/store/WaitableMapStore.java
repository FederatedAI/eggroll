package com.eggroll.core.deepspeed.store;

import com.eggroll.core.utils.JsonUtil;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class WaitableMapStore<K> {
    Logger log = LoggerFactory.getLogger(WaitableMapStore.class);
    
    private ConcurrentHashMap<K, Vector> store;

    public WaitableMapStore() {
        this.store = new ConcurrentHashMap<>();
    }

    public void set(K key, Vector value) {
        log.info("set key: " + key + ", value: " + value + ", store count: " + store.size());
        store.put(key, value);
        log.info("set key: " + key + ", value: " + value + ", store count: " + store.size() + " done");
    }

    public Vector get(K key, long timeout) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (!store.containsKey(key)) {
            log.info("waiting for key: " + key + ", store count: " + store.size());
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime > timeout) {
                log.info("Timeout after waiting for key: " + key + " for " + timeout + ", store count: " + store.size());
                return null;
            }
            Thread.sleep(1000);
        }
        return store.get(key);
    }

    private Vector longToV(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        byte[] byteArray = buffer.array();
        return new Vector<>(Arrays.asList(byteArray));
    }

    private long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }

    public long add(K key, long amount) {
        log.info("add key: " + key + ", amount: " + amount + ", store count: " + store.size());
        Vector result = store.compute(key, (k, v) -> {
            if (v == null) {
                return longToV(amount);
            } else {
                return longToV(bytesToLong(JsonUtil.convertToByteArray(v)) + amount);
            }
        });
        return bytesToLong(JsonUtil.convertToByteArray(result));
    }

    public void destroy() {
        store.clear();
    }
}