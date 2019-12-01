/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
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

package com.webank.eggroll.core.concurrent;


import com.webank.eggroll.core.model.StateTrackable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class AwaitSettableFuture<V> implements Future<V>, StateTrackable {
  private final CountDownLatch finishLatch;
  private V result;
  private Throwable cause;
  private AtomicInteger state;

  /**
   * The run state of this task, initially NEW.  The run state transitions to a terminal state only
   * in methods set, setException, and cancel.  During completion, state may take on transient
   * values of COMPLETING (while outcome is being set) or INTERRUPTING (only while interrupting the
   * runner to satisfy a cancel(true)). Transitions from these intermediate to final states use
   * cheaper ordered/lazy writes because values are unique and cannot be further modified.
   * <p>
   * Possible state transitions: NEW -> COMPLETING -> NORMAL NEW -> COMPLETING -> EXCEPTIONAL NEW ->
   * CANCELLED NEW -> INTERRUPTING -> INTERRUPTED
   */
  private static final int NEW = 0;
  private static final int COMPLETING = 1;
  private static final int NORMAL = 2;
  private static final int EXCEPTIONAL = 3;
  private static final int CANCELLED = 4;
  private static final int INTERRUPTING = 5;
  private static final int INTERRUPTED = 6;

  public AwaitSettableFuture() {
    this.finishLatch = new CountDownLatch(1);
    this.state = new AtomicInteger(NEW);
  }

  /**
   * Attempts to cancel execution of this task.  This attempt will fail if the task has already
   * completed, has already been cancelled, or could not be cancelled for some other reason. If
   * successful, and this task has not started when {@code cancel} is called, this task should never
   * run.  If the task has already started, then the {@code mayInterruptIfRunning} parameter
   * determines whether the thread executing this task should be interrupted in an attempt to stop
   * the task.
   *
   * <p>After this method returns, subsequent calls to {@link #isDone} will
   * always return {@code true}.  Subsequent calls to {@link #isCancelled} will always return {@code
   * true} if this method returned {@code true}.
   *
   * @param mayInterruptIfRunning {@code true} if the thread executing this task should be
   *                              interrupted; otherwise, in-progress tasks are allowed to complete
   * @return {@code false} if the task could not be cancelled, typically because it has already
   * completed normally; {@code true} otherwise
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    throw new UnsupportedOperationException("cancel operation not supported");
  }

  /**
   * Returns {@code true} if this task was cancelled before it completed normally.
   *
   * @return {@code true} if this task was cancelled before it completed
   */
  @Override
  public boolean isCancelled() {
    return state.get() == CANCELLED;
  }

  /**
   * Returns {@code true} if this task completed.
   * <p>
   * Completion may be due to normal termination, an exception, or cancellation -- in all of these
   * cases, this method will return {@code true}.
   *
   * @return {@code true} if this task completed
   */
  @Override
  public boolean isDone() {
    return state.get() != NEW;
  }

  /**
   * Waits if necessary for the computation to complete, and then retrieves its result.
   *
   * @return the computed result
   * @throws ExecutionException   if the computation threw an exception
   * @throws InterruptedException if the current thread was interrupted while waiting
   */
  @Override
  public V get() throws InterruptedException, ExecutionException {
    finishLatch.await();
    if (state.get() == NORMAL) {
      return result;
    }
    throw new ExecutionException("finish with error", cause);
  }

  /**
   * Waits if necessary for at most the given time for the computation to complete, and then
   * retrieves its result, if available.
   *
   * @param timeout the maximum time to wait
   * @param unit    the time unit of the timeout argument
   * @return the computed result
   * @throws ExecutionException   if the computation threw an exception
   * @throws InterruptedException if the current thread was interrupted while waiting
   * @throws TimeoutException     if the wait timed out
   */
  @Override
  public V get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    boolean awaitResult = finishLatch.await(timeout, unit);

    if (awaitResult) {
      if (state.get() == NORMAL) {
        return result;
      } else {
        throw new ExecutionException("finish with error", cause);
      }
    } else {
      throw new TimeoutException("result get timeout");
    }

  }

  public V getNow() {
    if (state.get() == NORMAL) {
      return result;
    }

    throw new IllegalStateException("No result has been set yet");
  }

  public boolean isNormal() {
    return state.get() == NORMAL;
  }

  public void setResult(V result) {
    if (state.compareAndSet(NEW, COMPLETING)) {
      this.result = result;
      state.compareAndSet(COMPLETING, NORMAL);
      finishLatch.countDown();
    } else {
      throw new IllegalStateException("error setting normal result in state: " + state.get());
    }
  }

  public void setError(Throwable cause) {
    if (state.compareAndSet(NEW, COMPLETING)) {
      this.cause = cause;
      state.compareAndSet(COMPLETING, EXCEPTIONAL);
      finishLatch.countDown();
    } else {
      throw new IllegalStateException("error setting error in state: " + state.get());
    }
  }

  @Override
  public boolean hasError() {
    if (state.get() == EXCEPTIONAL) {
      if (cause != null) {
        return true;
      } else {
        throw new IllegalStateException("state is EXCEPTIONAL but cause is null");
      }
    } else {
      return false;
    }
  }

  @Override
  public Throwable getError() {
    return cause;
  }
}
