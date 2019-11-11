package com.webank.eggroll.core.datastructure

import java.util
import java.util.concurrent.{ArrayBlockingQueue, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.webank.eggroll.core.util.Logging
import javax.annotation.concurrent.GuardedBy


trait Broker[E] extends AutoCloseable {
  def isWriteFinished(): Boolean

  def signalWriteFinish(): Unit
  def getRemainingWriteSignalCount(): Int
  def isReadReady(): Boolean
  def isClosable(): Boolean
  def total(): Long
  def totalNull(): Long
  def size(): Long

  def put(e: E): Unit
  def take(): E

  def offer(e: E, timeout: Long, unit: TimeUnit): Unit
  def poll(timeout: Long, unit: TimeUnit): E

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

  override def size(): Long = queue.size()

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

  override def offer(e: E, timeout: Long, unit: TimeUnit): Unit = queue.offer(e, timeout, unit)

  override def poll(timeout: Long, unit: TimeUnit): E = queue.poll(timeout, unit)
}

object LinkedBlockingBroker {
  val namePrefix = "ArrayBlockingBroker-"
  val brokerSeq = new AtomicLong(1L)
}