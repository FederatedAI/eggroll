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
 *
 *
 */

package com.webank.eggroll.core.datastructure

import java.util
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, LinkedBlockingQueue, TimeUnit}

import com.webank.eggroll.core.util.Logging


trait Broker[E] extends AutoCloseable with Iterator[E] {
  def isWriteFinished(): Boolean

  def signalWriteFinish(): Unit
  def getRemainingWriteSignalCount(): Int
  def isReadReady(): Boolean
  def isClosable(): Boolean
  def total(): Long
  def totalNull(): Long
  def size(): Int

  // blocking forever until successful
  def put(e: E): Unit
  def take(): E

  // blocking, wait until timeout, returns special value
  def offer(e: E, timeout: Long, unit: TimeUnit): Boolean
  def poll(timeout: Long, unit: TimeUnit): E

  // non-blocking, returns special value
  def offer(e: E): Boolean
  def poll(): E
  def peek(): E

  def drainTo(target: util.Collection[E], maxElements: Int = 10000): Unit
}


class LinkedBlockingBroker[E](maxCapacity: Int = 10000,
                              writeSignals: Int = 1,
                              name: String = s"${LinkedBlockingBroker.namePrefix}${System.currentTimeMillis()}-${LinkedBlockingBroker.brokerSeq.getAndIncrement()}") extends Broker[E] with Logging {
  val queue = new LinkedBlockingQueue[E](maxCapacity)
  var writeFinished = false
  val historyTotal = new AtomicLong(0L)
  val historyNullTotal = new AtomicLong(0L)
  val remainingSignalCount = new AtomicInteger(writeSignals)

  override def isWriteFinished(): Boolean = this.synchronized(writeFinished)

  override def signalWriteFinish(): Unit = this.synchronized {
    if (isWriteFinished()) {
      throw new IllegalStateException(s"finish signaling overflows. initial value: ${writeSignals}")
    }

    if (remainingSignalCount.decrementAndGet() <= 0) {
      writeFinished = true
    }
  }

  override def isReadReady(): Boolean = queue.size() > 0

  override def put(e: E): Unit = {
    if (!isWriteFinished()) {
      queue.put(e)
      historyTotal.incrementAndGet()
      if (e == null) {
        historyNullTotal.incrementAndGet()
      }
    } else {
      throw new IllegalStateException(s"putting into a write-finised broker: ${name}")
    }
  }

  override def take(): E = queue.take()

  override def isClosable(): Boolean = isWriteFinished() && !isReadReady()

  override def total(): Long = historyTotal.get()

  override def size(): Int = queue.size()

  override def close(): Unit = {
    if (!isClosable()) {
      throw new IllegalStateException(s"broker is not closable: ${name}. " +
        s"write status: ${writeFinished}, read remaining: ${size()}. " +
        s"history total: ${historyTotal}, null total: ${historyNullTotal}")
    }
  }

  override def totalNull(): Long = historyNullTotal.incrementAndGet()

  override def drainTo(target: util.Collection[E], maxElements: Int = 10000): Unit = this.synchronized(queue.drainTo(target, maxElements))

  override def getRemainingWriteSignalCount(): Int = remainingSignalCount.get()

  override def offer(e: E, timeout: Long, unit: TimeUnit): Boolean = queue.offer(e, timeout, unit)

  override def poll(timeout: Long, unit: TimeUnit): E = queue.poll(timeout, unit)

  override def offer(e: E): Boolean = queue.offer(e)

  override def poll(): E = queue.poll()

  override def peek(): E = queue.peek()

  override def hasNext: Boolean = !isClosable()

  override def next(): E = take()
}

object LinkedBlockingBroker {
  val namePrefix = "ArrayBlockingBroker-"
  val brokerSeq = new AtomicLong(1L)
}



class FifoBroker[E](maxSize: Int = 100, writers: Int = 1, name: String = "") extends Iterator[E] {
  val broker = new ArrayBlockingQueue[E](maxSize)
  private val remainingWriters = new CountDownLatch(writers)
  private val lock = new ReentrantLock()

  override def hasNext: Boolean = {
    while (true) {
      if (!broker.isEmpty) {
        return true
      } else {
        if (getRemainingWritersCount() <= 0) return false
        else Thread.sleep(10) // continue
      }
    }

    throw new IllegalStateException(
      s"FifoBroker should not get here name=${name}," +
        s"maxSize=${maxSize}, " +
        s"writers=${writers}, " +
        s"remainingWriters=${remainingWriters.getCount}")
  }

  override def next(): E = broker.take()

  def signalWriteFinish(): Unit = {
    if (remainingWriters.getCount > 0) remainingWriters.countDown()
    else throw new IllegalStateException(
      s"FifoBroker name=${name} countdown underflow." +
        s"maxSize=${maxSize}, " +
        s"writers=${writers}, " +
        s"remainingWriters=${remainingWriters.getCount}")
  }

  def getRemainingWritersCount(): Long = remainingWriters.getCount
}