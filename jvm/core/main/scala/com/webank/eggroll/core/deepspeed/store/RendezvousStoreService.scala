package com.webank.eggroll.core.deepspeed.store

import com.webank.eggroll.core.constant.SessionStatus
import com.webank.eggroll.core.deepspeed.store.meta._
import com.webank.eggroll.core.resourcemanager.SessionMetaDao
import com.webank.eggroll.core.util.Logging

import java.util.concurrent.ConcurrentHashMap

class RendezvousStoreService extends Logging {

  val stores = new ConcurrentHashMap[String, WaitableMapStore]()
  private val smDao = new SessionMetaDao


  new Thread(() => {
    while (true) {
      Thread.sleep(60000)
      val storeSize = stores.size()
      logInfo(s"store size: $storeSize")
      if (stores.size() > 10) {
        val prefix = stores.keys()
        while (prefix.hasMoreElements) {
          val key = prefix.nextElement()
          try {
            smDao.getSession(key).status match {
              case SessionStatus.NEW_TIMEOUT =>
              case SessionStatus.ERROR =>
              case SessionStatus.KILLED =>
              case SessionStatus.CLOSED =>
              case SessionStatus.FINISHED =>
                destroyStore(key)
              case _ =>
            }
          } catch {
            case e: Exception =>
              logWarning(s"session $key not found")
              destroyStore(key)
          }
        }
      }
    }
  }).start()

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
      RendezvousStoreGetResponse(value.get, isTimeout = false)
    } else {
      RendezvousStoreGetResponse(Vector.empty, isTimeout = true)
    }
  }

  def add(request: RendezvousStoreAddRequest): RendezvousStoreAddResponse = {
    val store = getStore(request.prefix)
    val amount = store.add(request.key, request.amount)
    RendezvousStoreAddResponse(amount)
  }
}
