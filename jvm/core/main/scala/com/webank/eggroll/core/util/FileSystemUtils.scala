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

import com.google.common.base.Preconditions
import com.webank.eggroll.core.constant.StringConstants

import java.io.{File, PrintWriter}
import scala.io.Source

object FileSystemUtils {
  val parentDirRegex = "\\.\\."

  def stripParentDirReference(path: String): String = {
    Preconditions.checkNotNull(path)
    path.replaceAll(parentDirRegex, StringConstants.EMPTY)
  }
  def fileWriter(fileName: String, content: String): Unit = {
    val writer = new PrintWriter(new File(fileName))
    writer.write(content)
    writer.close()
  }

  def  fileReader(fileName:String):String = {
       Source.fromFile(fileName).mkString;  //using mkString method

  }





}
