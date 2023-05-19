package com.webank.eggroll.core.deepspeed.store

import com.webank.eggroll.core.deepspeed.store.meta._
import com.webank.eggroll.core.util.Logging

import java.util.concurrent.ConcurrentHashMap

class RendezvousStoreService extends Logging {

  val stores = new ConcurrentHashMap[String, WaitableMapStore]()

  private def getStore(prefix: String): WaitableMapStore = {
    stores.computeIfAbsent(prefix, _ => new WaitableMapStore)
  }

  def destroyStore(prefix: String): Unit = {
    val store = stores.remove(prefix)
    if (store != null) {
      store.destroy()
    }
  }

  def set(request: RendezvousStoreSetRequest): RendezvousStoreSetResponse = {
    val store = getStore(request.prefix)
    store.set(request.key, request.value)
    RendezvousStoreSetResponse()
  }

  def get(request: RendezvousStoreGetRequest): RendezvousStoreGetResponse = {
    val store = getStore(request.prefix)
    val value = store.get(request.key, request.timeout)
    if (value.isDefined) {
      RendezvousStoreGetResponse(value.get)
    } else {
      throw new Exception(s"key ${request.key} not found")
    }
  }

  def add(request: RendezvousStoreAddRequest): RendezvousStoreAddResponse = {
    val store = getStore(request.prefix)
    val amount = store.add(request.key, request.amount)
    RendezvousStoreAddResponse(amount)
  }
}
