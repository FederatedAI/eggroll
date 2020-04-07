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

import com.google.common.base.Preconditions;
import com.webank.eggroll.rollsite.infra.Pipe;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class BasePipe implements Pipe {
    private final CountDownLatch closeLatch;
    private CountDownLatch writerLatch;
    private volatile boolean closed = false;
    private volatile boolean drained = false;
    private volatile String type = "";
    private volatile String tag_key = "";
    private volatile Throwable throwable = null;
    private int initWriterLatchCount;

    public BasePipe() {
        this.closeLatch = new CountDownLatch(1);
    }

    @Override
    public Object read() {
        throw new UnsupportedOperationException("Operation not implemented");
    }

    @Override
    public void write(Object o) {
        throw new UnsupportedOperationException("Operation not implemented");
    }

    @Override
    public void onError(Throwable t) {
        setDrained();
        close();
        this.throwable = t;
    }

    @Override
    public void onComplete() {
        // setDrained();
        close();
    }

    @Override
    public synchronized void close() {
        setDrained();
        closeLatch.countDown();
        this.closed = true;
    }

    @Override
    public boolean isClosed() {
/*        if (!closed) {
            if ((writerLatch == null || writerLatch.getCount() == 0) && isDrained()) {
                close();
            }
        }*/
        return closed;
    }

    @Override
    public void setDrained() {
        this.drained = true;
    }

    @Override
    public boolean isDrained() {
        return this.drained;
    }

    @Override
    public void awaitClosed() throws InterruptedException {
        closeLatch.await();
    }

    @Override
    public void awaitClosed(long timeout, TimeUnit unit) throws InterruptedException {
        closeLatch.await(timeout, unit);
    }

    @Override
    public boolean hasError() {
        return throwable != null;
    }

    @Override
    public Throwable getError() {
        return throwable;
    }

    public String getType() {
        return this.type;
    }
    public void setType(String type) {
        this.type = type;
    }

    public String getTagKey() {
        return this.tag_key;
    }
    public void setTagKey(String tag_key) {
        this.tag_key = tag_key;
    }

    public void checkNotClosed() {
        if (isClosed()) {
            throw new IllegalStateException("pipe closed");
        }
    }

    @Override
    public void initWriters(int writersCount) {
        if (!isWritersInited()) {
            synchronized (this) {
                if (!isWritersInited()) {
                    Preconditions.checkArgument(writersCount > 0, "writer count must > 0");

                    if (writerLatch == null) {
                        writerLatch = new CountDownLatch(writersCount);
                        initWriterLatchCount = writersCount;
                    } else if (writersCount != initWriterLatchCount) {
                        throw new IllegalStateException("duplicate resetting writerLatch");
                    }
                }
            }
        }
    }

    @Override
    public void signalWriteFinish() {
        writerLatch.countDown();
    }

    @Override
    public boolean isWriteFinished() {
        return writerLatch.getCount() <= 0;
    }

    @Override
    public boolean isWritersInited() {
        return writerLatch != null;
    }
}
