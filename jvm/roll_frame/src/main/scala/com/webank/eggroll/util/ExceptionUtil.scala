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
 */

package com.webank.eggroll.util

import java.io.{PrintWriter, StringWriter}

import scala.collection.mutable

class ExceptionUtil {

}

class ThrowableCollection extends RuntimeException{
  private val causes = mutable.ListBuffer[Throwable]()
  def append(t: Throwable):Unit = causes.append(t)
  def check():Unit = {
    if(causes.nonEmpty){
      throw this
    }
  }

  override def toString: String = {
    val writer = new StringWriter()
    causes.foreach(_.printStackTrace(new PrintWriter(writer)))
    writer.toString
  }
}