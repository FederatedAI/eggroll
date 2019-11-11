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

package com.webank.eggroll.rollframe

import com.webank.eggroll.core.command.CommandURI
import com.webank.eggroll.core.meta.{ErJob, ErStore}
import com.webank.eggroll.core.schedule.{BaseTaskPlan, JobRunner, ListScheduler}

class RollFrameService() extends RollFrame {
  val scheduler = new ListScheduler
  val clusterManager = new ClusterManager
  //val collectiveCommand = new CollectiveCommand(clusterManager.getServerCluster("test1").nodes)

  /*
    private def getResultStore(output: RfStore, parts: List[RfPartition]):RfStore ={
      val ret = if(output == null) {
        RfStore("temp-store-" + Math.abs(new Random().nextLong()), input.namespace, parts.size)
      } else {
        output
      }
      ret.partitions = parts
      ret
    }
  */

  private def getResultStore(input: ErStore, output: ErStore = null): ErStore = {
    val finalOutput = if (output == null) {
      val outputStoreLocator = input.storeLocator.copy(name = s"temp-store-${System.nanoTime()}")

      ErStore(storeLocator = outputStoreLocator,
        partitions = input.partitions.map(p => p.copy(storeLocator = outputStoreLocator)))
    } else {
      output
    }

    finalOutput
  }

  // implicit def string2CommandURI(s: String): CommandURI = new CommandURI(s)

  def mapBatches(job: ErJob): ErStore = {
    // todo: deal with output
    run(new MapBatchTask(uri = new CommandURI("EggFrame.runTask"), job = job))
  }

  def reduce(job: ErJob): ErStore = {
    run(new ReduceBatchTask(uri = new CommandURI("EggFrame.runTask"), job = job))
  }

  def aggregate(job: ErJob): ErStore = {
    run(new AggregateBatchTask(uri = new CommandURI("EggFrame.runTask"), job = job))
  }

  def run(taskPlan: BaseTaskPlan): ErStore = {
    scheduler.addPlan(taskPlan)
    JobRunner.run(scheduler.getPlan())
    taskPlan.job.outputs.head
  }

  def companionEgg():EggFrame = new EggFrame

}

