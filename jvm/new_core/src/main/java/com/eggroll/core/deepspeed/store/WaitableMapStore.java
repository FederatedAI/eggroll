package com.eggroll.core.deepspeed.store;

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class WaitableMapStore {
    Logger log = LoggerFactory.getLogger(WaitableMapStore.class);
    
    private ConcurrentHashMap<byte[], byte[]> store = new ConcurrentHashMap<>();

    public void set(byte[] key, byte[] value) {
//        log.debug("set key: " + Arrays.toString(key) + ", value: " + Arrays.toString(value) + ", store count: " + store.size());
        store.put(key, value);
//        log.debug("set key: " + Arrays.toString(key) + ", value: " + Arrays.toString(value) + ", store count: " + store.size() + " done");
    }

    public byte[] get(byte[] key, int timeout) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (!store.containsKey(key)) {
//            log.debug("waiting for key: " + Arrays.toString(key) + ", store count: " + store.size());
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (elapsedTime > timeout) {
                log.debug("Timeout after waiting for key: " + Arrays.toString(key) + " for " + timeout + ", store count: " + store.size());
                return null;
            }
            Thread.sleep(1000);
        }
        return store.get(key);
    }

    private byte[] longToV(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private long vToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }

    public long add(byte[] key, long amount) {
//        log.debug("add key: " + Arrays.toString(key) + ", amount: " + amount + ", store count: " + store.size());
        byte[] compute = store.compute(key, (k, v) -> {
            if (v == null) {
                return longToV(amount);
            } else {
                return longToV(vToLong(v) + amount);
            }
        });
        return vToLong(compute);
    }

    public void destroy() {
        store.clear();
    }
}