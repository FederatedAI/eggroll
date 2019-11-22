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

package com.webank.eggroll.core.schedule

import com.webank.eggroll.core.command.CollectiveCommand
import com.webank.eggroll.core.datastructure.TaskPlan
import com.webank.eggroll.core.meta.ErTask
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.core.util.Logging

import scala.collection.mutable

trait Scheduler extends Logging {

}



// todo: add another layer of abstraction if coupling with ErJob is proved a bad practice or
//  communication (e.g. broadcast), or io operation should be described in task plan as computing
case class ListScheduler() extends Scheduler {
  private val stages = mutable.Queue[TaskPlan]()
  private val functorSerdes = DefaultScalaSerdes()

  def addPlan(plan: TaskPlan): ListScheduler = {
    stages += plan
    this
  }

  def getPlan(): TaskPlan = {
    stages.dequeue()
  }

}

object JobRunner {
  def run(plan: TaskPlan): Array[ErTask] = {
    val collectiveCommand = CollectiveCommand(plan)
    collectiveCommand.call()
  }
}