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

package com.webank.ai.eggroll.core.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractIterator<T> implements Iterator<T> {

    private State state = State.NOT_READY;
    private T next;

    @Override
    public boolean hasNext() {
        switch (state) {
            case FAILED:
                throw new IllegalStateException("Iterator is in failed state");
            case DONE:
                return false;
            case READY:
                return true;
            default:
                return maybeComputeNext();
        }
    }

    @Override
    public T next() {
        if (!hasNext())
            throw new NoSuchElementException();
        state = State.NOT_READY;
        if (next == null)
            throw new IllegalStateException("Expected item but none found.");
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Removal not supported");
    }

    public T peek() {
        if (!hasNext())
            throw new NoSuchElementException();
        return next;
    }

    protected T allDone() {
        state = State.DONE;
        return null;
    }

    protected abstract T makeNext();

    private Boolean maybeComputeNext() {
        state = State.FAILED;
        next = makeNext();
        if (state == State.DONE) {
            return false;
        } else {
            state = State.READY;
            return true;
        }
    }

    private enum State {
        READY, NOT_READY, DONE, FAILED
    }

}