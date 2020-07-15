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
import com.webank.eggroll.core.util.ToStringUtils;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class PacketQueuePipe extends BasePipe {
    private static final Logger LOGGER = LogManager.getLogger(PacketQueuePipe.class);

    private Proxy.Metadata metadata;
    private LinkedBlockingQueue<Proxy.Packet> queue = new LinkedBlockingQueue<>(3000);
    private int inCounter = 0;
    private int outCounter = 0;

    public PacketQueuePipe() {
        this(null);
    }

    public PacketQueuePipe(Proxy.Metadata metadata) {
        super();
        // this.queue = queueFactory.createConcurrentLinkedQueue();
        this.metadata = metadata;
    }

    @Override
    public Proxy.Packet read() {
        // LOGGER.info("read for the {} time, queue size: {}", ++inCounter, queue.size());
        return queue.poll();
    }

    @Override
    public Proxy.Packet read(long timeout, TimeUnit unit) {
        Proxy.Packet result = null;
        if (isDrained()) return result;
        try {
            result = queue.poll(timeout, unit);
        } catch (InterruptedException e) {
            LOGGER.debug("read wait timeout. metadata={}", ToStringUtils.toOneLineString(this.metadata));
            Thread.currentThread().interrupt();
        }

        return result;
    }

    @Override
    public void write(Object o) {
        // LOGGER.info("write for the {} time, queue size: {}", ++outCounter, queue.size());
        if (o instanceof Proxy.Packet) {
            queue.add((Proxy.Packet) o);
        } else {
            throw new IllegalArgumentException("object o is of type: " + o.getClass().getCanonicalName()
                    + ", which is not of type " + Proxy.Packet.class.getCanonicalName());
        }
    }

    @Override
    public boolean isDrained() {
        return super.isDrained() && queue.isEmpty();
    }

    @Override
    public synchronized void close() {
        super.close();
    }

    public int getQueueSize() {
        return queue.size();
    }
}
