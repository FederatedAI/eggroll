package com.webank.eggroll.rollsite

import com.webank.eggroll.core.constant.RollSiteConfKeys
import io.grpc._
import org.apache.commons.lang3.StringUtils
import scala.collection.mutable.ArrayBuffer

object WhiteList {
  private var whiteList: Array[String] = Array()

  def init(): Unit = {
    val whiteListConf = RollSiteConfKeys.EGGROLL_ROLLSITE_ROUTE_TABLE_WHITELIST.get()
    init(whiteListConf)
  }

  def init(str: String): Unit = {
    if (!StringUtils.isBlank(str)) {
      val whiteListArray = str.split("\\,")
      val arrBuf = ArrayBuffer[String]()
      whiteListArray.foreach((elem: String) => {
        // e.g: 127.0.0.1/5
        if (elem.contains("/")){
          val ipSeg = elem.split("/")
          if (ipSeg.length != 2) {
            throw new IllegalArgumentException("whitelist config error: %s".format(elem))
          }
          val startSeg = ipSeg(0)
          val lastDotIndex = startSeg.lastIndexOf(".")
          for (i <- startSeg.substring(lastDotIndex +1).toInt to ipSeg(1).toInt) {
            arrBuf.append(startSeg.substring(0, lastDotIndex+1) + i)
          }
        }else{
          arrBuf.append(elem)
        }
      })
      this.whiteList = arrBuf.toArray
    }
  }

  def check(ip: String): Boolean = {
    if (whiteList.nonEmpty) {
      if (whiteList.contains(ip)) {
        return true
      }
    }
    false
  }

  def main(args: Array[String]): Unit = {
    // init("127.0.0.1,127.0.0.2")
    // println(check("127.0.0.1"))

  }
}

object AddrAuthServerInterceptor {
  val REMOTE_ADDR: Context.Key[AnyRef] = Context.key("remoteAddr")
}

class AddrAuthServerInterceptor extends ServerInterceptor {
  override def interceptCall[ReqT, RespT](call: ServerCall[ReqT, RespT], headers: Metadata,
                                          next: ServerCallHandler[ReqT, RespT]): ServerCall.Listener[ReqT] = {
    val remoteAddr = call.getAttributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString
    val remoteAddrSplited = remoteAddr.split(":")
    val context = Context.current.withValue(AddrAuthServerInterceptor.REMOTE_ADDR,
      remoteAddrSplited(0).replaceAll("\\/", ""))
    Contexts.interceptCall(context, call, headers, next)
  }
}
