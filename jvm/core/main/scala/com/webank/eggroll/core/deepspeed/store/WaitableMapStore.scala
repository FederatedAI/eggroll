package com.webank.eggroll.core.deepspeed.store

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise}
import scala.util.Try

class WaitableMapStore {
  private val store = new ConcurrentHashMap[K, V]()
  private val promiseMap = new ConcurrentHashMap[K, Promise[V]]()

  def set(key: K, value: V): Unit = {
    store.put(key, value)
    Option(promiseMap.remove(key)).foreach(_.success(value))
  }

  def get(key: K, timeout: Duration): Option[V] = {
    Option(store.get(key)).orElse {
      val newPromise = Promise[V]()
      val existingPromise = promiseMap.putIfAbsent(key, newPromise)

      val promiseToUse = if (existingPromise == null) newPromise else existingPromise
      val future = promiseToUse.future
      Try(Await.result(future, timeout)).toOption
    }
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
    promiseMap.clear()
  }
}