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

package com.webank.eggroll.rollpair.component

import com.webank.eggroll.core.command.{CollectiveCommand, CommandRouter, CommandURI}
import com.webank.eggroll.core.constant.StringConstants
import com.webank.eggroll.core.meta.{ErFunctor, ErJob, ErStore}
import com.webank.eggroll.core.serdes.DefaultScalaFunctorSerdes

class RollPair(erStore: ErStore) {
  val functorSerDes = DefaultScalaFunctorSerdes()

  def mapValues(f: Array[Byte] => Array[Byte]): RollPair = {
    val functor = ErFunctor("map_user_defined", functorSerDes.serialize(f))

    val inputLocator = erStore.storeLocator

    val outputStore = erStore.copy(storeLocator = inputLocator.copy(name = "testoutput"))

    val job = ErJob("mapValues", List(erStore), List(outputStore), List(functor))

    val collectiveCommand = CollectiveCommand(new CommandURI(RollPair.mapCommand), job)

    val commandResults = collectiveCommand.call()

    new RollPair(job.outputs.head)
  }
}

object RollPair {
  val clazz = classOf[RollPair]
  var mapCommand: String = _
  mapCommand = clazz.getCanonicalName + StringConstants.DOT + "mapValues"
  CommandRouter.register(mapCommand,
    List(classOf[Array[Byte] => Array[Byte]]), clazz, "mapValues", null, null)
}