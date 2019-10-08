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

package com.webank.ai.eggroll.framework.roll.api.grpc.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.webank.ai.eggroll.api.storage.Kv;
import com.webank.ai.eggroll.api.storage.StorageBasic;
import com.webank.ai.eggroll.core.factory.GrpcServerFactory;
import com.webank.ai.eggroll.core.io.StoreInfo;
import com.webank.ai.eggroll.core.model.Bytes;
import com.webank.ai.eggroll.core.server.ServerConf;
import com.webank.ai.eggroll.core.utils.ToStringUtils;
import com.webank.ai.eggroll.framework.meta.service.dao.generated.model.Node;
import com.webank.ai.eggroll.framework.roll.service.model.OperandBroker;
import com.webank.ai.eggroll.framework.storage.service.model.enums.Stores;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:applicationContext-roll.xml"})
public class TestEggKvServiceClient {
    private static final Logger LOGGER = LogManager.getLogger();
    @Autowired
    private StorageServiceClient storageServiceClient;
    @Autowired
    private StorageServiceClient anotherServiceClient;
    @Autowired
    private ToStringUtils toStringUtils;
    @Autowired
    private GrpcServerFactory grpcServerFactory;
    @Autowired
    private ServerConf serverConf;


    private String name = "api_create_name";
    private String namespace = "api_create_namespace";
    private String jobid1 = "jobid1";
    private String clusterCommTable = "__clustercomm__";
    private StoreInfo storeInfo;
    private Node node;

    @Before
    public void init() throws Exception {
        serverConf = grpcServerFactory.parseConfFile("/Users/max-webank/git/floss/project/eggroll/roll/src/main/resources/roll.properties");
        storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();
        node = new Node();
        node.setIp("127.0.0.1");
        node.setPort(7778);
    }

    @Test
    public void testPut() {
        Kv.Operand.Builder operandBuilder = Kv.Operand.newBuilder();

        Kv.Operand operand = operandBuilder.setKey(ByteString.copyFromUtf8("lemon")).setValue(ByteString.copyFromUtf8("tea")).build();

        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();

        storageServiceClient.put(operand, storeInfo, node);
    }

    @Test
    public void testPutIfAbsent() {
        Kv.Operand.Builder operandBuilder = Kv.Operand.newBuilder();

        Kv.Operand operand = operandBuilder.setKey(ByteString.copyFromUtf8("time")).setValue(ByteString.copyFromUtf8(String.valueOf(System.currentTimeMillis()))).build();

        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();

        Kv.Operand result = storageServiceClient.putIfAbsent(operand, storeInfo, node);

        LOGGER.info("putIfAbsent result: {}", result);
    }

    @Test
    public void testPutAll() {
        OperandBroker operandBroker = new OperandBroker();
        Kv.Operand.Builder operandBuilder = Kv.Operand.newBuilder();
        Kv.Operand operand1 = operandBuilder.setKey(ByteString.copyFromUtf8("time")).setValue(ByteString.copyFromUtf8(String.valueOf(System.currentTimeMillis()))).build();
        Kv.Operand operand2 = operandBuilder.setKey(ByteString.copyFromUtf8("key")).setValue(ByteString.copyFromUtf8("value")).build();
        Kv.Operand operand3 = operandBuilder.setKey(ByteString.copyFromUtf8("hello")).setValue(ByteString.copyFromUtf8("world")).build();
        Kv.Operand operand4 = operandBuilder.setKey(ByteString.copyFromUtf8("iter")).setValue(ByteString.copyFromUtf8("ator")).build();
        Kv.Operand operand5 = operandBuilder.setKey(ByteString.copyFromUtf8("happy")).setValue(ByteString.copyFromUtf8("holidays")).build();

        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LMDB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();

        operandBroker.put(operand1);
        operandBroker.put(operand2);
        operandBroker.put(operand3);
        operandBroker.put(operand4);
        operandBroker.put(operand5);

        operandBroker.setFinished();
        storageServiceClient.putAll(operandBroker, storeInfo, node);
    }

    @Test
    public void testPutAllToSameDb() {
        OperandBroker operandBroker = new OperandBroker();
        Kv.Operand.Builder operandBuilder = Kv.Operand.newBuilder();

        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LMDB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();

        Kv.Operand operand = null;
        for (int i = 0; i < 100; ++i) {
            //operand = operandBuilder.setKey(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(20))).setValue(ByteString.copyFromUtf8("v" + i)).build();
            operand = operandBuilder.setKey(ByteString.copyFromUtf8("k" + i)).setValue(ByteString.copyFromUtf8("v" + i)).build();
            operandBroker.put(operand);
        }

        operandBroker.setFinished();
        storageServiceClient.putAll(operandBroker, storeInfo, node);

        operandBroker = new OperandBroker();
        for (int i = 1000; i < 1100; ++i) {
            operand = operandBuilder.setKey(ByteString.copyFromUtf8("k" + i)).setValue(ByteString.copyFromUtf8("v" + i)).build();
            operandBroker.put(operand);
        }

        operandBroker.setFinished();
        storageServiceClient.putAll(operandBroker, storeInfo, node);
    }

    @Test
    public void testPutAllMany() throws Exception {
        OperandBroker operandBroker = new OperandBroker();
        Kv.Operand.Builder operandBuilder = Kv.Operand.newBuilder();

        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                storageServiceClient.putAll(operandBroker, storeInfo, node);
            }
        });

        thread.start();

        Kv.Operand operand = null;
        int resetInterval = 10000;
        int curCount = 0;
        for (int i = 0; i < 100000; ++i) {
            if (curCount <= 0) {
                curCount = resetInterval;
                LOGGER.info("current: {}", i);
            }

            --curCount;
            //operand = operandBuilder.setKey(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(20))).setValue(ByteString.copyFromUtf8("v" + i)).build();
            operand = operandBuilder.setKey(ByteString.copyFromUtf8("k" + i)).setValue(ByteString.copyFromUtf8("v" + i)).build();
            operandBroker.put(operand);
        }

        operandBroker.setFinished();

        thread.join();
        //rollKvServiceClient.putAll(operandBroker, storeInfo);
    }

    @Test
    public void testDelete() {
        Kv.Operand.Builder operandBuilder = Kv.Operand.newBuilder();

        Kv.Operand operand = operandBuilder.setKey(ByteString.copyFromUtf8("lemon")).setValue(ByteString.copyFromUtf8(String.valueOf(System.currentTimeMillis()))).build();

        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();

        storageServiceClient.put(operand, storeInfo, node);
        Kv.Operand result = storageServiceClient.delete(operand, storeInfo, node);

        LOGGER.info("delete result: {}", result);
    }

    @Test
    public void testGet() {
        Kv.Operand.Builder operandBuilder = Kv.Operand.newBuilder();

        Kv.Operand operand = operandBuilder.setKey(ByteString.copyFromUtf8("lemon")).setValue(ByteString.copyFromUtf8(String.valueOf(System.currentTimeMillis()))).build();

        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();

        Kv.Operand result = storageServiceClient.get(operand, storeInfo, node);

        LOGGER.info("get result: {}", result);
    }

    @Test
    public void testMultipleGet() {
        Kv.Operand.Builder operandBuilder = Kv.Operand.newBuilder();

        Kv.Operand operand = operandBuilder.setKey(ByteString.copyFromUtf8("k0")).setValue(ByteString.copyFromUtf8(String.valueOf(System.currentTimeMillis()))).build();

        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();
        Kv.Operand result = null;
        for (int i = 0; i < 100000; ++i) {
            result = storageServiceClient.get(operand, storeInfo, node);
        }
        LOGGER.info("get result: {}", result);
    }

    @Test
    public void testIterate() throws Exception {
        Kv.Range range = Kv.Range.newBuilder().setStart(ByteString.copyFromUtf8("k99999")).setEnd(ByteString.copyFromUtf8("")).setMinChunkSize(100).build();
        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();
        OperandBroker operandBroker = storageServiceClient.iterate(range, storeInfo, node);
        operandBroker = storageServiceClient.iterate(range, storeInfo, node);

        List<Kv.Operand> operands = Lists.newLinkedList();

        int count = 0;
        Kv.Operand previous = null;
        Kv.Operand current = null;
        int correctCount = 0;
        while (!operandBroker.isClosable()) {
            operandBroker.awaitLatch(1, TimeUnit.SECONDS);
            operandBroker.drainTo(operands);
            count += operands.size();

            for (Kv.Operand operand : operands) {
                previous = current;
                current = operand;

                if (previous != null && current != null) {
                    Bytes previousBytes = Bytes.wrap(previous.getKey());
                    Bytes currentBytes = Bytes.wrap(current.getKey());

                    if (previousBytes.compareTo(currentBytes) < 0) {
                        ++correctCount;
                    }
                }
                // LOGGER.info("operand: {}", toStringUtils.toOneLineString(operand));
                // LOGGER.info("key: {}, value: {}", operand.getKey().toStringUtf8(), operand.getValue().toStringUtf8());
            }
            operands.clear();
        }

        LOGGER.info("iterate count: {}", count);
        LOGGER.info("correct count: {}", correctCount);
    }

    @Test
    public void testIterateSegment() throws Exception {
        Kv.Range range = Kv.Range.newBuilder().setStart(ByteString.copyFromUtf8("")).setEnd(ByteString.copyFromUtf8("")).setMinChunkSize(1000).build();
        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();

        Set<String> bsSet = Sets.newConcurrentHashSet();
        int batchCount = 0;
        int count = 0;
        int correctCount = 0;
        Kv.Operand previous = null;
        Kv.Operand current = null;
        LinkedList<Kv.Operand> operands = Lists.newLinkedList();

        boolean hasProcessed = false;
        while (!hasProcessed || !operands.isEmpty()) {
            hasProcessed = true;
            operands.clear();
            LOGGER.info("range start: {}, end: {}, count: {}, correct count: {}, set count: {}",
                    range.getStart().toStringUtf8(), range.getEnd().toStringUtf8(), count, correctCount, bsSet.size());
            OperandBroker operandBroker = storageServiceClient.iterate(range, storeInfo, node);

            while (!operandBroker.isClosable()) {
                operandBroker.awaitLatch(1, TimeUnit.SECONDS);
                operandBroker.drainTo(operands);
                count += operands.size();

                for (Kv.Operand operand : operands) {
                    previous = current;
                    current = operand;
                    bsSet.add(operand.getKey().toStringUtf8());
                    if (previous != null && current != null) {
                        Bytes previousBytes = Bytes.wrap(previous.getKey());
                        Bytes currentBytes = Bytes.wrap(current.getKey());

                        if (previousBytes.compareTo(currentBytes) < 0) {
                            ++correctCount;
                        }
                    }
                    // LOGGER.info("operand: {}", toStringUtils.toOneLineString(operand));
                    // LOGGER.info("key: {}, value: {}", operand.getKey().toStringUtf8(), operand.getValue().toStringUtf8());

                }
                Kv.Operand last = operands.getLast();
                range = range.toBuilder().setStart(last.getKey()).build();

            }
            LOGGER.info("iterate count: {}, set size: {}, correct count: {}", count, bsSet.size(), correctCount);
        }
    }

    @Test
    public void testDestroy() {
        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();

        storageServiceClient.destroy(Kv.Empty.getDefaultInstance(), storeInfo, node);

        LOGGER.info("done destroy");
    }

    @Test
    public void testCount() {
        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();
        Kv.Count result = storageServiceClient.count(Kv.Empty.getDefaultInstance(), storeInfo, node);
        LOGGER.info("count result: {}", result.getValue());
    }

    @Test
    public void testBigData() {
        ByteString key = ByteString.copyFromUtf8("1M");
        Kv.Operand.Builder operandBuilder = Kv.Operand.newBuilder();
        operandBuilder.setKey(key)
                .setValue(ByteString.copyFromUtf8(StringUtils.repeat("1", 10000000)));

        storageServiceClient.put(operandBuilder.build(), storeInfo, node);

        LOGGER.info("put finished");
        Kv.Operand result = storageServiceClient.get(operandBuilder.clear().setKey(key).build(), storeInfo, node);
        LOGGER.info("get done. length: {}", result.getValue().size());
    }

    @Test
    public void testMultiPutAll() throws Exception {
        OperandBroker operandBroker1 = new OperandBroker();
        Kv.Operand.Builder operandBuilder1 = Kv.Operand.newBuilder();

        OperandBroker operandBroker2 = new OperandBroker();
        Kv.Operand.Builder operandBuilder2 = Kv.Operand.newBuilder();

        StoreInfo storeInfo = StoreInfo.builder()
                .type(Stores.LEVEL_DB.name())
                .nameSpace(namespace)
                .tableName(name)
                .build();

        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                storageServiceClient.putAll(operandBroker1, storeInfo, node);
            }
        });

        Thread thread2 = new Thread(() -> {
            anotherServiceClient.putAll(operandBroker2, storeInfo, node);
        });

        Kv.Operand operand = null;
        int resetInterval = 10000;
        int curCount = 0;
        for (int i = 0; i < 100000; ++i) {
            if (curCount <= 0) {
                curCount = resetInterval;
                LOGGER.info("current: {}", i);
            }

            --curCount;
            //operand = operandBuilder.setKey(ByteString.copyFromUtf8(RandomStringUtils.randomAlphanumeric(20))).setValue(ByteString.copyFromUtf8("v" + i)).build();
            operand = operandBuilder1.setKey(ByteString.copyFromUtf8("k" + i)).setValue(ByteString.copyFromUtf8("v" + i)).build();
            operandBroker1.put(operand);

            operand = operandBuilder1.setKey(ByteString.copyFromUtf8("K" + i)).setValue(ByteString.copyFromUtf8("V" + i)).build();
            operandBroker2.put(operand);
        }

        operandBroker1.setFinished();
        operandBroker2.setFinished();

        thread1.start();

        thread2.start();
        thread1.join();
        thread2.join();

        Kv.Count result = storageServiceClient.count(Kv.Empty.getDefaultInstance(), storeInfo, node);
        LOGGER.info("count result: {}", result.getValue());
        //rollKvServiceClient.putAll(operandBroker, storeInfo);
    }
}
