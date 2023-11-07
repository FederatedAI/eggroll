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

    private ConcurrentHashMap<String, byte[]> store = new ConcurrentHashMap<>();

    public void set(String key, byte[] value) {
        log.info("set key: " + key + ", value: " + Arrays.toString(value) + ", store count: " + store.size());
        store.put(key, value);
        log.info("set key: " + key + ", value: " + Arrays.toString(value) + ", store count: " + store.size() + " done");
    }

    public byte[] get(String key, long timeout) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (!store.containsKey(key)) {
            log.info("waiting for key: {}, timeout {} store count: {}",key,timeout, store.size());
            long elapsedTime = (System.currentTimeMillis() - startTime)/1000;
            if (elapsedTime > timeout) {
                log.error("Timeout after waiting for key: " + key + " for " + timeout + ", store count: " + store.size());
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

    public long add(String key, long amount) {

        byte[] compute = store.compute(key, (k, v) -> {
            if (v == null) {
                return longToV(amount);
            } else {
                return longToV(vToLong(v) + amount);
            }
        });
        long result = vToLong(compute);
        log.info("add key: " + key + ", amount: " + amount + ",return " + result + " store count: " + store.size());
        return result;
    }

    public void destroy() {
        store.clear();
    }

    public  static void main(String[] args){
        System.err.println(992/100);
    }
}