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
import com.webank.eggroll.core.meta.ErJob
import com.webank.eggroll.core.schedule.{BaseTaskPlan, JobRunner, ListScheduler}

class RollFrameService() extends RollFrame {
  val scheduler = new ListScheduler
  val clusterManager = new ClusterManager

  def mapBatches(job: ErJob): ErJob = {
    // todo: deal with output
    run(new MapBatchTask(uri = new CommandURI("EggFrame.runTask"), job = job))
  }

  def reduce(job: ErJob): ErJob = {
    run(new ReduceBatchTask(uri = new CommandURI("EggFrame.runTask"), job = job))
  }

  def aggregate(job: ErJob): ErJob = {
    run(new AggregateBatchTask(uri = new CommandURI("EggFrame.runTask"), job = job))
  }

  def run(taskPlan: BaseTaskPlan): ErJob = {
    scheduler.addPlan(taskPlan)
    JobRunner.run(scheduler.getPlan())

    taskPlan.job
  }

  def companionEgg(): EggFrame = new EggFrame

}

