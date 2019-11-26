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

package com.webank.eggroll.rollsite.factory;

import com.google.common.collect.Maps;
import com.webank.ai.eggroll.api.networking.proxy.Proxy;
import com.webank.eggroll.rollsite.infra.Pipe;
import com.webank.eggroll.rollsite.infra.impl.InputStreamOutputStreamNoStoragePipe;
import com.webank.eggroll.rollsite.infra.impl.InputStreamToPacketUnidirectionalPipe;
import com.webank.eggroll.rollsite.infra.impl.PacketQueuePipe;
import com.webank.eggroll.rollsite.infra.impl.PacketQueueSingleResultPipe;
import com.webank.eggroll.rollsite.infra.impl.PacketToOutputStreamUnidirectionalPipe;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.io.OutputStream;

@Component("defaultPipeFactory")
public class DefaultPipeFactory implements PipeFactory {
    private static final Logger LOGGER = LogManager.getLogger(DefaultPipeFactory.class);
    @Autowired
    private LocalBeanFactory localBeanFactory;

    private static Map<String, PacketQueueSingleResultPipe> pipeMap = Maps.newConcurrentMap();

    public DefaultPipeFactory() {
        LOGGER.info("DefaultPipeFactory construct");
        //pipeMap = Maps.newConcurrentMap();
    }


    public InputStreamOutputStreamNoStoragePipe createInputStreamOutputStreamNoStoragePipe(InputStream is,
                                                                                           OutputStream os,
                                                                                           Proxy.Metadata metadata) {
        return (InputStreamOutputStreamNoStoragePipe) localBeanFactory
                .getBean(InputStreamOutputStreamNoStoragePipe.class, is, os, metadata);
    }

    public InputStreamToPacketUnidirectionalPipe createInputStreamToPacketUnidirectionalPipe(InputStream is,
                                                                                             Proxy.Metadata metadata) {
        return (InputStreamToPacketUnidirectionalPipe) localBeanFactory
                .getBean(InputStreamToPacketUnidirectionalPipe.class, is, metadata);
    }

    public InputStreamToPacketUnidirectionalPipe createInputStreamToPacketUnidirectionalPipe(InputStream is,
                                                                                             Proxy.Metadata metadata,
                                                                                             int trunkSize) {
        return (InputStreamToPacketUnidirectionalPipe) localBeanFactory
                .getBean(InputStreamToPacketUnidirectionalPipe.class, is, metadata, trunkSize);
    }

    public PacketToOutputStreamUnidirectionalPipe createPacketToOutputStreamUnidirectionalPipe(OutputStream os) {
        return (PacketToOutputStreamUnidirectionalPipe) localBeanFactory
                .getBean(PacketToOutputStreamUnidirectionalPipe.class, os);
    }

    public PacketQueuePipe createPacketQueuePipe(Proxy.Metadata metadata) {
        return (PacketQueuePipe) localBeanFactory.getBean(PacketQueuePipe.class, metadata);
    }

    @Override
    public Pipe create(String name) {
        PacketQueueSingleResultPipe pipe;
        LOGGER.info("pipeMap: {}", pipeMap);
        synchronized(this) {
            if (pipeMap.containsKey(name)) {
                LOGGER.info("key {} exited", name);
                pipe = pipeMap.get(name);
            }
            else {
                pipe = (PacketQueueSingleResultPipe) localBeanFactory.getBean(PacketQueueSingleResultPipe.class);
                pipeMap.put(name, pipe);

                LOGGER.info("create key {}", name);
            }
        }

        return pipe;
    }
}
