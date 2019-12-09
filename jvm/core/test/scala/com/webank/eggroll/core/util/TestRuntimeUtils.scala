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

import java.net.{Inet4Address, ServerSocket}

import com.google.common.net.InetAddresses
import com.webank.eggroll.core.session.StaticErConf
import org.scalatest.FlatSpec

class TestRuntimeUtils extends FlatSpec {
  val port = 60000
  "An available port" should "return true" in {
    val result = RuntimeUtils.isPortAvailable(port)
    assert(result)
  }

  it should "return false" in {
    val ss: ServerSocket = new ServerSocket(port)
    val result = RuntimeUtils.isPortAvailable(port)
    ss.close()

    assert(!result)
  }

  "RuntimeUtils.getMySiteLocalAddress" should "return a valid site local ip" in {
    val ip = RuntimeUtils.getMySiteLocalAddress()
    assert(InetAddresses.isInetAddress(ip))
  }

  it should "return a valid ipv4 site local ip" in {
    val ip = RuntimeUtils.getMySiteLocalAddress(forceIpV4 = true)
    assert(InetAddresses.forString(ip).isInstanceOf[Inet4Address])
  }

  "RuntimeUtils" should "return ip and port for a conf" in {
    StaticErConf.setPort(port)
    val ipAndPort = RuntimeUtils.getMySiteLocalAddressAndPort(true)
    println(ipAndPort)
  }
}
