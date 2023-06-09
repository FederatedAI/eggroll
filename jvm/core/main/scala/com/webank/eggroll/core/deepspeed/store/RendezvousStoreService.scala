package com.webank.eggroll.core.deepspeed.store

import com.webank.eggroll.core.deepspeed.store.meta._
import com.webank.eggroll.core.util.Logging

import java.util.concurrent.ConcurrentHashMap

class RendezvousStoreService extends Logging {

  val stores = new ConcurrentHashMap[String, WaitableMapStore]()
  //  private val smDao = new SessionMetaDao


  //  new Thread(() => {
  //    while (true) {
  //      Thread.sleep(60000)
  //      val storeSize = stores.size()
  //      logInfo(s"store size: $storeSize")
  //      if (stores.size() > 10) {
  //        val prefix = stores.keys()
  //        while (prefix.hasMoreElements) {
  //          val key = prefix.nextElement()
  //          try {
  //            smDao.getSession(key).status match {
  //              case SessionStatus.NEW_TIMEOUT =>
  //              case SessionStatus.ERROR =>
  //              case SessionStatus.KILLED =>
  //              case SessionStatus.CLOSED =>
  //              case SessionStatus.FINISHED =>
  //                destroyStore(key)
  //              case _ =>
  //            }
  //          } catch {
  //            case e: Exception =>
  //              logWarning(s"session $key not found")
  //              destroyStore(key)
  //          }
  //        }
  //      }
  //    }
  //  }).start()

  private def getStore(prefix: String): WaitableMapStore = {
    logDebug(s"getStore: $prefix")
    val store = stores.computeIfAbsent(prefix, _ => new WaitableMapStore)
    logDebug(s"getStore: $prefix done, store: $store")
    store
  }

  private def destroyStore(prefix: String): Boolean = {
    val store = stores.remove(prefix)
    if (store != null) {
      store.destroy()
      true
    } else {
      false
    }
  }

  def destroy(rendezvousStoreDestroyRequest: RendezvousStoreDestroyRequest): RendezvousStoreDestroyResponse = {
    logDebug(s"destroy: $rendezvousStoreDestroyRequest")
    val success = destroyStore(rendezvousStoreDestroyRequest.prefix)
    logDebug(s"destroy: $rendezvousStoreDestroyRequest done, success: $success")
    RendezvousStoreDestroyResponse(success = success)
  }

  def set(request: RendezvousStoreSetRequest): RendezvousStoreSetResponse = {
    val store = getStore(request.prefix)
    logDebug(s"set: $request to store $store")
    store.set(request.key, request.value)
    logDebug(s"set: $request done")
    RendezvousStoreSetResponse()
  }

  def get(request: RendezvousStoreGetRequest): RendezvousStoreGetResponse = {
    logDebug(s"get: $request to store $stores")
    val store = getStore(request.prefix)
    val value = store.get(request.key, request.timeout)
    if (value.isDefined) {
      logDebug(s"get: $request done")
      RendezvousStoreGetResponse(value.get, isTimeout = false)
    } else {
      logDebug(s"get: $request timeout")
      RendezvousStoreGetResponse(Vector.empty, isTimeout = true)
    }
  }

  def add(request: RendezvousStoreAddRequest): RendezvousStoreAddResponse = {
    logDebug(s"add: $request to store $stores")
    val store = getStore(request.prefix)
    val amount = store.add(request.key, request.amount)
    logDebug(s"add: $request done")
    RendezvousStoreAddResponse(amount)
  }
}
