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

package com.webank.eggroll.rollpair

import java.lang.reflect.Constructor

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.command.CommandURI
import com.webank.eggroll.core.constant.ClusterManagerConfKeys
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.schedule._
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.core.session.StaticErConf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RollPairMaster() {
  val scheduler = ListScheduler()
  lazy val clusterManagerClient = new ClusterManagerClient()
  val cmPort: Int = StaticErConf.getProperty(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, "0").toInt

  def runJob(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head

    val isOutputSpecified = inputJob.outputs.nonEmpty
    val finalInputs = ArrayBuffer[ErStore]()
    StaticErConf.addProperty(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_PORT, cmPort.toString)
    finalInputs.sizeHint(inputJob.inputs.length)
    inputJob.inputs.foreach(input => {
      val inputStoreWithPartitions = clusterManagerClient.getStore(input)
      if (inputStoreWithPartitions == null) {
        val sl = input.storeLocator
        throw new IllegalArgumentException(s"input store ${sl.storeType}-${sl.namespace}-${sl.name} does not exist")
      }
      finalInputs += inputStoreWithPartitions
    })
    val finalInputTemplate = finalInputs.head
    val outputTotalPartitions =
      if (inputJob.outputs.isEmpty) finalInputTemplate.storeLocator.totalPartitions
      else inputJob.outputs.head.storeLocator.totalPartitions
    val outputStoreProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = outputTotalPartitions)
        ErStore(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      val outputStoreLocator = finalInputTemplate.storeLocator.fork()
      ErStore(storeLocator = outputStoreLocator.copy(totalPartitions = outputTotalPartitions))
    }
    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreProposal)
    val taskPlanJob = inputJob.copy(inputs = finalInputs.toArray, outputs = Array(outputStoreWithPartitions))

    val taskPlanConstructor = RollPairMaster.taskPlanConstructors.getOrElseUpdate(inputJob.name, {
      val constructor = Class.forName(s"com.webank.eggroll.core.schedule.${inputJob.name.capitalize}TaskPlan")
        .getConstructor(classOf[CommandURI], classOf[ErJob])
        .asInstanceOf[Constructor[BaseTaskPlan]]
      RollPairMaster.taskPlanConstructors += (inputJob.name -> constructor)
      constructor
    })

    val taskPlan: BaseTaskPlan = taskPlanConstructor.newInstance(RollPair.EGG_RUN_TASK_COMMAND, taskPlanJob)

    JobRunner.run(taskPlan)

    taskPlanJob
  }
}

object RollPairMaster {
  private val taskPlanConstructors = mutable.Map[String, Constructor[BaseTaskPlan]]()
  val functorSerDes = DefaultScalaSerdes()
}