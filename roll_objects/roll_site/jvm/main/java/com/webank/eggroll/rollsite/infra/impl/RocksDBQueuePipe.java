/*
 * Copyright 2019 The Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.eggroll.rollsite.infra.impl;


import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.rollsite.factory.QueueFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.rocksdb.*;

@Component
@Scope("prototype")
public class RocksDBQueuePipe extends BasePipe {
    private static final Logger LOGGER = LogManager.getLogger(RocksDBQueuePipe.class);
    @Autowired
    private QueueFactory queueFactory;
    private Proxy.Metadata metadata;
    private LinkedBlockingQueue<Proxy.Packet> queue;

    private static final String dbPath = "/Users/weiwei/Documents/rocksdb/java/data/";
    private static ByteBuffer buffer = ByteBuffer.allocate(8);

    static {
        RocksDB.loadLibrary();
    }

    RocksDB rocksDB;

    private long capacity;
    private long firstIndex;
    private AtomicLong currentIndex;

    public RocksDBQueuePipe() {
        this(null);
    }

    public RocksDBQueuePipe(Proxy.Metadata metadata) {
        super();
        // this.queue = queueFactory.createConcurrentLinkedQueue();
        this.metadata = metadata;

        Options options = new Options();
        options.setCreateIfMissing(true);

        if (!Files.isSymbolicLink(Paths.get(dbPath))) {
            try {
                Files.createDirectories(Paths.get(dbPath));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            rocksDB = RocksDB.open(options, dbPath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        try {
            capacity = rocksDB.getAggregatedLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        byte[] getValue;
        try {
            getValue = rocksDB.get("index_queue_first_index".getBytes());
            if(getValue.length != 0) {
                firstIndex = bytesToLong(getValue);
                System.out.println("IndexQueue111, firstIndex:" + firstIndex);
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        currentIndex = new AtomicLong(capacity + firstIndex);
    }

    @PostConstruct
    private void init() {
        this.queue = queueFactory.createLinkedBlockingQueue(3000);
    }


    public static byte[] longToBytes(long x) {
        buffer.putLong(0, x);
        return buffer.array();
    }

    public static long bytesToLong(byte[] bytes) {
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static Object unserialize(byte[] bytes) {
        ByteArrayInputStream bais = null;
        try {
            bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (Exception e) {

        }
        return null;
    }

    public static byte[] serialize(Object object) {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            byte[] bytes = baos.toByteArray();
            return bytes;
        } catch (Exception e) {
            System.err.println("serialize failed");
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public Proxy.Packet read() {
        // LOGGER.info("read for the {} time, queue size: {}", ++inCounter, queue.size());
        byte[] getValue = new byte[0];
        try {
            getValue = rocksDB.get(longToBytes(firstIndex));
            System.out.println(new String(getValue));
            return (Proxy.Packet)unserialize(getValue);
        } catch (RocksDBException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public Proxy.Packet read(long timeout, TimeUnit unit) {
        /*
        byte[] getValue = new byte[0];
        try {
            getValue = rocksDB.get(longToBytes(firstIndex));
            System.out.println(new String(getValue));
            return (Proxy.Packet)unserialize(getValue);
        } catch (RocksDBException e) {
            e.printStackTrace();
            return null;
        }
        */
        return null;
    }

    @Override
    public void write(Object o) {
        long i = currentIndex.getAndIncrement();
        System.out.println("offer:" + i);
        try {
            rocksDB.put(longToBytes(i), serialize(o));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean isDrained() {
        return super.isDrained() && queue.isEmpty();
    }

    @Override
    public synchronized void close() {
        super.close();
        if (rocksDB != null) {
            rocksDB.close();
            rocksDB = null;
        }
    }

    public int getQueueSize() {
        return queue.size();
    }
}
