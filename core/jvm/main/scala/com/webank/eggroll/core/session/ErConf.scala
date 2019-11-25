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

package com.webank.eggroll.core.session

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

import com.webank.eggroll.core.constant.StringConstants
import org.apache.commons.beanutils.BeanUtils

import scala.collection.mutable

abstract class ErConf {
  protected val conf: Properties = new Properties()
  private val confRepository: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()

  def getProperties(): Properties = {
    val duplicateConf: Properties = new Properties()
    BeanUtils.copyProperties(getConf(), duplicateConf)
    duplicateConf
  }

  def getProperty(key: String, defaultValue: Any, forceReload: Boolean = false): String = {
    var result: String = null
    val value = confRepository get key

    if (forceReload || value.isEmpty) {
      val resultRef = getConf().get(key)

      if (resultRef != null) {
        result = resultRef.toString
        confRepository + (key -> result)
      } else {
        result = defaultValue.toString
      }
    } else {
      result = value.get
    }

    result
  }

  def getLong(key: String, defaultValue: Long): Long = {
    getProperty(key, defaultValue).toLong
  }

  def getInt(key: String, defaultValue: Int): Int = {
    getProperty(key, defaultValue).toInt
  }

  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getProperty(key, defaultValue).toBoolean
  }

  def getString(key: String, defaultValue: String = StringConstants.EMPTY): String = {
    getProperty(key, defaultValue)
  }

  def getPort(): Int

  def getModuleName(): String

  def addProperties(prop: Properties): ErConf = {
    getConf().putAll(prop)
    this
  }

  def addProperties(path: String): ErConf = {
    val prop = new Properties

    val fis = new BufferedInputStream(new FileInputStream(path))
    prop.load(fis)

    addProperties(prop)
  }

  def addProperty(key: String, value: String): ErConf = {
    getConf().setProperty(key, value)
    this
  }

  def get[T](key: String, defaultValue: T): T = {
    val result = getConf().get(key)

    if (result != null) {
      result.asInstanceOf[T]
    } else {
      defaultValue
    }
  }

  protected def getConf(): Properties = {
    this.conf
  }
}

case class RuntimeErConf(prop: Properties = new Properties()) extends ErConf {
  override protected val conf = new Properties(super.getConf())
  conf.putAll(prop)

  override def getPort(): Int = StaticErConf.getPort()

  override def getModuleName(): String = StaticErConf.getModuleName()

  override protected def getConf(): Properties = conf
}

object StaticErConf extends ErConf {
  var port: Int = -1
  var moduleName: String = _

  def setPort(port: Int): StaticErConf.type = {
    this.port match {
      case -1 => this.port = port
      case _ => throw new IllegalStateException("port has already been set")
    }

    this
  }

  override def getPort(): Int = {
    port
  }

  def setModuleName(moduleName: String): Unit = {
    this.moduleName match {
      case null => this.moduleName = moduleName
      case _ => throw new IllegalStateException("module name has already been set")
    }
  }

  override def getModuleName(): String = {
    moduleName
  }
}