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

package com.webank.eggroll.rollsite

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch}

trait Broker {

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
