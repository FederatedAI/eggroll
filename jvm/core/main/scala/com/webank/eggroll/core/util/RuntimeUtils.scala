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

package com.webank.eggroll.core.util

import java.io.IOException
import java.net._
import java.util

import com.webank.eggroll.core.constant.NetworkConstants
import com.webank.eggroll.core.session.StaticErConf
import org.apache.commons.lang3.StringUtils

object RuntimeUtils extends Logging {
  var siteLocalAddress: String = null
  var siteLocalAddressAndPort: String = null

  def isValidLocalAddress(ni: NetworkInterface, addr: InetAddress): Boolean = {
    addr != null && !addr.isLoopbackAddress && (ni.isPointToPoint || !addr.isLinkLocalAddress)
  }

  private def getSiteLocalAddressInternal(forceIpV4: Boolean = false): InetAddress = {
    var result: InetAddress = null
    if (siteLocalAddress == null) {
      val os = System.getProperty("os.name").toLowerCase

      if (StringUtils.containsAny(os, "nix", "nux", "mac")) {
        val nis: util.Enumeration[NetworkInterface] = NetworkInterface.getNetworkInterfaces

        if (nis == null) {
          return result
        }

        var ni: NetworkInterface = null
        var addr: InetAddress = null
        var addrs: util.Enumeration[InetAddress] = null
        while (nis.hasMoreElements) {
          ni = nis.nextElement
          addrs = ni.getInetAddresses

          while (addrs.hasMoreElements) {
            addr = addrs.nextElement
            if (!forceIpV4 || addr.isInstanceOf[Inet4Address]) {
              if (isValidLocalAddress(ni, addr)) {
                result = addr
                return result
              }
            }
          }
        }
      } else {
        result = InetAddress.getLocalHost
      }
    }

    result
  }

  def getMySiteLocalAddress(forceIpV4: Boolean = false): String = {
    if (siteLocalAddress == null) {
      try {
        val addr: InetAddress = getSiteLocalAddressInternal(forceIpV4)
        if (addr == null) {
          siteLocalAddress = NetworkConstants.DEFAULT_LOCALHOST_IP
        } else {
          siteLocalAddress = addr.getHostAddress
          if (addr.isInstanceOf[Inet6Address]) {
            siteLocalAddress = siteLocalAddress.split('%')(0)
          }
        }
      } catch {
        case e@(_: SocketException | _: UnknownHostException) => {
          siteLocalAddress = NetworkConstants.DEFAULT_LOCALHOST_IP
        }
      }
    }

    siteLocalAddress
  }

  def getMySiteLocalAddressAndPort(forceIpV4: Boolean = false): String = {
    if (siteLocalAddressAndPort == null) {
      siteLocalAddressAndPort = getMySiteLocalAddress(forceIpV4) + ":" + StaticErConf.getPort()
    }

    siteLocalAddressAndPort
  }

  def isPortAvailable(port: Int): Boolean = {
    if (port <= 0) return false
    var ignored: ServerSocket = null
    try {
      ignored = new ServerSocket(port)
    } catch {
      case e: IOException => {
        return false
      }
    } finally {
      if (ignored != null) {
        ignored.close()
      }
    }
    true
  }

  def getPidOfProcess(p: Process): Long = {
    var pid = -1L
    try {
      if (p.getClass.getName == "java.lang.UNIXProcess") {
        val f = p.getClass.getDeclaredField("pid")
        f.setAccessible(true)
        pid = f.getLong(p)
        f.setAccessible(false)
      }
    } catch {
      case e: Exception =>
        pid = -1L
    }
    pid
  }
}


