package com.webank.eggroll.core.deepspeed.store

import com.webank.eggroll.core.util.Logging

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.Duration

class WaitableMapStore extends Logging {
  private val store = new ConcurrentHashMap[K, V]()
  //  private val promiseMap = new ConcurrentHashMap[K, Promise[V]]()

  def set(key: K, value: V): Unit = {
    logInfo(s"set key: $key, value: $value, store count: ${store.size()}")
    store.put(key, value)
    logInfo(s"set key: $key, value: $value, store count: ${store.size()} done")
    //    Option(promiseMap.remove(key)).foreach(_.success(value))
  }

  def get(key: K, timeout: Duration): Option[V] = {
    val startTime = System.currentTimeMillis()
    while (!store.containsKey(key)) {
      logDebug(s"waiting for key: $key, store count: ${store.size()}")
      val elapsedTime = System.currentTimeMillis() - startTime
      if (elapsedTime > timeout.toMillis) {
        logDebug(s"Timeout after waiting for key: $key for $timeout, store count: ${store.size()}")
        return None
      }
      Thread.sleep(1000)
    }
    Option(store.get(key))
    //    Option(store.get(key)).orElse {
    //      val newPromise = Promise[V]()
    //      val existingPromise = promiseMap.putIfAbsent(key, newPromise)
    //
    //      val promiseToUse = if (existingPromise == null) newPromise else existingPromise
    //      val future = promiseToUse.future
    //      Try(Await.result(future, timeout)).toOption
    //    }
  }

  private def longToV(x: Long): V = {
    val buffer = java.nio.ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.putLong(x)
    buffer.array().toVector
  }

  private def vToLong(x: V): Long = {
    val buffer = java.nio.ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.put(x.toArray)
    buffer.flip()
    buffer.getLong
  }

  def add(key: K, amount: Long): Long = {
    logInfo(s"add key: $key, amount: $amount, store count: ${store.size()}")
    vToLong(store.compute(key, (_, v) => {
      if (v == null) {
        longToV(amount)
      } else {
        longToV(vToLong(v) + amount)
      }
    }))
  }

  def destroy(): Unit = {
    store.clear()
    //    promiseMap.clear()
  }
}