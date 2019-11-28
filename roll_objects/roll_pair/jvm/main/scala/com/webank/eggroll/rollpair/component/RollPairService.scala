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

package com.webank.eggroll.rollpair.component

import com.webank.eggroll.core.command.CommandURI
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.schedule.{FilterTaskPlan, FlatMapTaskPlan, GlomTaskPlan, SampleTaskPlan, SubtractByKeyTaskPlan, UnionTaskPlan, _}
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import com.webank.eggroll.framework.clustermanager.client.ClusterManagerClient
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable

class RollPairService() {
  val scheduler = ListScheduler()
  val clusterManagerClient = new ClusterManagerClient()

  def mapValues(inputJob: ErJob): ErJob = {
    // f: Array[Byte] => Array[Byte]
    // val functor = ErFunctor("map_user_defined", functorSerDes.serialize(f))

    val inputStore = inputJob.inputs.head
    val isOutputSpecified = inputJob.outputs.nonEmpty

    val inputStoreWithPartitions = clusterManagerClient.getStore(inputStore)

    val outputStoreProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = inputStoreWithPartitions.storeLocator.totalPartitions)
        inputStoreWithPartitions.fork(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      inputStoreWithPartitions.fork()
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(inputStoreWithPartitions), outputs = Array(outputStoreWithPartitions))
    val taskPlan = new MapTaskPlan(new CommandURI(RollPairService.eggMapValuesCommand), taskPlanJob)

    scheduler.addPlan(taskPlan)

    // todo: update database
    // val xxx = updateOutputDb()

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  // todo: give default partition function: hash and mod
  def map(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head
    val inputLocator = inputStore.storeLocator
    val outputLocator = inputLocator.copy(name = "testMap")

    val inputPartitionTemplate = ErPartition(id = 0, storeLocator = inputLocator, processor = ErProcessor(commandEndpoint = ErEndpoint("localhost", 20001)))
    val outputPartitionTemplate = ErPartition(id = 0, storeLocator = outputLocator, processor = ErProcessor(commandEndpoint = ErEndpoint("localhost", 20001)))

    val numberOfPartitions = 4

    val inputPartitions = mutable.ArrayBuffer[ErPartition]()
    val outputPartitions = mutable.ArrayBuffer[ErPartition]()

    for (i <- 0 until numberOfPartitions) {
      inputPartitions += inputPartitionTemplate.copy(id = i)
      outputPartitions += outputPartitionTemplate.copy(id = i)
    }

    val inputStoreWithPartitions = inputStore.copy(storeLocator = inputLocator,
      partitions = inputPartitions.toArray)
    val outputStoreWithPartitions = inputStore.copy(storeLocator = outputLocator,
      partitions = outputPartitions.toArray)

    val job = inputJob.copy(inputs = Array(inputStoreWithPartitions), outputs = Array(outputStoreWithPartitions))

    val taskPlan = new ShuffleTaskPlan(new CommandURI(RollPairService.eggMapCommand), job)
    scheduler.addPlan(taskPlan)

    JobRunner.run(scheduler.getPlan())

    job
  }

  def reduce(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head
    val isOutputSpecified = inputJob.outputs.nonEmpty
    val inputStoreWithPartitions = clusterManagerClient.getStore(inputStore)

    val outputStoreProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = 1)
        ErStore(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      val outputStoreLocator = inputStore.storeLocator.fork()
      ErStore(storeLocator = outputStoreLocator.copy(totalPartitions = 1))
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(inputStoreWithPartitions), outputs = Array(outputStoreWithPartitions))

    val taskPlan = new ReduceTaskPlan(new CommandURI(RollPairService.eggReduceCommand), taskPlanJob)
    scheduler.addPlan(taskPlan)

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  def mapPartitions(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head
    val isOutputSpecified = inputJob.outputs.nonEmpty

    val inputStoreWithPartitions = clusterManagerClient.getStore(inputStore)

    val outputStoreWithPartitionProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = inputStoreWithPartitions.storeLocator.totalPartitions)
        inputStoreWithPartitions.fork(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      inputStoreWithPartitions.fork()
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreWithPartitionProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(inputStoreWithPartitions), outputs = Array(outputStoreWithPartitions))
    val taskPlan = new MapTaskPlan(new CommandURI(RollPairService.eggMapPartitionsCommand), taskPlanJob)

    scheduler.addPlan(taskPlan)

    // todo: update database
    // val xxx = updateOutputDb()

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  def collapsePartitions(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head
    val isOutputSpecified = inputJob.outputs.nonEmpty

    val inputStoreWithPartitions = clusterManagerClient.getStore(inputStore)

    val outputStoreWithPartitionProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = inputStoreWithPartitions.storeLocator.totalPartitions)
        inputStoreWithPartitions.fork(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      inputStoreWithPartitions.fork()
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreWithPartitionProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(inputStoreWithPartitions), outputs = Array(outputStoreWithPartitions))
    val taskPlan = new MapTaskPlan(new CommandURI(RollPairService.eggCollapsePartitionsCommand), taskPlanJob)

    scheduler.addPlan(taskPlan)

    // todo: update database
    // val xxx = updateOutputDb()

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  def flatMap(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head
    val isOutputSpecified = inputJob.outputs.nonEmpty

    val inputStoreWithPartitions = clusterManagerClient.getStore(inputStore)

    val outputStoreWithPartitionProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = inputStoreWithPartitions.storeLocator.totalPartitions)
        inputStoreWithPartitions.fork(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      inputStoreWithPartitions.fork()
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreWithPartitionProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(inputStoreWithPartitions), outputs = Array(outputStoreWithPartitions))
    val taskPlan = new FlatMapTaskPlan(new CommandURI(RollPairService.eggFlatMapCommand), taskPlanJob)

    scheduler.addPlan(taskPlan)

    // todo: update database
    // val xxx = updateOutputDb()

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  def glom(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head
    val isOutputSpecified = inputJob.outputs.nonEmpty

    val inputStoreWithPartitions = clusterManagerClient.getStore(inputStore)

    val outputStoreWithPartitionProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = inputStoreWithPartitions.storeLocator.totalPartitions)
        inputStoreWithPartitions.fork(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      inputStoreWithPartitions.fork()
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreWithPartitionProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(inputStoreWithPartitions), outputs = Array(outputStoreWithPartitions))
    val taskPlan = new GlomTaskPlan(new CommandURI(RollPairService.eggGlomMapCommand), taskPlanJob)

    scheduler.addPlan(taskPlan)

    // todo: update database
    // val xxx = updateOutputDb()

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  def sample(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head
    val isOutputSpecified = inputJob.outputs.nonEmpty

    val inputStoreWithPartitions = clusterManagerClient.getStore(inputStore)

    val outputStoreWithPartitionProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = inputStoreWithPartitions.storeLocator.totalPartitions)
        inputStoreWithPartitions.fork(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      inputStoreWithPartitions.fork()
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreWithPartitionProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(inputStoreWithPartitions), outputs = Array(outputStoreWithPartitions))
    val taskPlan = new SampleTaskPlan(new CommandURI(RollPairService.eggSampleMapCommand), taskPlanJob)

    scheduler.addPlan(taskPlan)

    // todo: update database
    // val xxx = updateOutputDb()

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  def filter(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head
    val isOutputSpecified = inputJob.outputs.nonEmpty

    val inputStoreWithPartitions = clusterManagerClient.getStore(inputStore)

    val outputStoreWithPartitionProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = inputStoreWithPartitions.storeLocator.totalPartitions)
        inputStoreWithPartitions.fork(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      inputStoreWithPartitions.fork()
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreWithPartitionProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(inputStoreWithPartitions), outputs = Array(outputStoreWithPartitions))
    val taskPlan = new FilterTaskPlan(new CommandURI(RollPairService.eggFilterMapCommand), taskPlanJob)

    scheduler.addPlan(taskPlan)

    // todo: update database
    // val xxx = updateOutputDb()

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  def subtractByKey(inputJob: ErJob): ErJob = {
    val leftStore = inputJob.inputs.head
    val leftStoreWithPartitions = clusterManagerClient.getStore(leftStore)

    val rightStore = inputJob.inputs(1)
    val rightStoreWithPartitions = clusterManagerClient.getStore(rightStore)
    val isOutputSpecified = inputJob.outputs.nonEmpty

    val outputStoreProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputTotalPartitions =
          if (StringUtils.equalsAny(RollPairService.reduce, RollPairService.aggregate)) 1
          else leftStoreWithPartitions.storeLocator.totalPartitions

        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = outputTotalPartitions)
        ErStore(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      val outputStoreLocator = leftStore.storeLocator.fork()
      ErStore(storeLocator = outputStoreLocator.copy(totalPartitions = 1))
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(leftStoreWithPartitions, rightStoreWithPartitions), outputs = Array(outputStoreWithPartitions))
    val taskPlan = new SubtractByKeyTaskPlan(new CommandURI(RollPairService.eggSubtractByKeyCommand), taskPlanJob)
    scheduler.addPlan(taskPlan)

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  def union(inputJob: ErJob): ErJob = {
    val leftStore = inputJob.inputs.head
    val leftStoreWithPartitions = clusterManagerClient.getStore(leftStore)

    val rightStore = inputJob.inputs(1)
    val rightStoreWithPartitions = clusterManagerClient.getStore(rightStore)
    val isOutputSpecified = inputJob.outputs.nonEmpty

    val outputStoreProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputTotalPartitions =
          if (StringUtils.equalsAny(RollPairService.reduce, RollPairService.aggregate)) 1
          else leftStoreWithPartitions.storeLocator.totalPartitions

        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = outputTotalPartitions)
        ErStore(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      val outputStoreLocator = leftStore.storeLocator.fork()
      ErStore(storeLocator = outputStoreLocator.copy(totalPartitions = 1))
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(leftStoreWithPartitions, rightStoreWithPartitions), outputs = Array(outputStoreWithPartitions))
    val taskPlan = new UnionTaskPlan(new CommandURI(RollPairService.eggUnionCommand), taskPlanJob)
    scheduler.addPlan(taskPlan)

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  // todo: compact all jobs to this call
  def runJob(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head

    val isOutputSpecified = inputJob.outputs.nonEmpty
    val inputStoreWithPartitions = clusterManagerClient.getStore(inputStore)

    // todo: totalPartitions for aggregate / non-aggregate jobs
    val outputStoreProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputTotalPartitions =
          if (StringUtils.equalsAny(RollPairService.reduce, RollPairService.aggregate)) 1
          else inputStoreWithPartitions.storeLocator.totalPartitions

        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = outputTotalPartitions)
        ErStore(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      val outputStoreLocator = inputStore.storeLocator.fork()
      ErStore(storeLocator = outputStoreLocator.copy(totalPartitions = 1))
    }
    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(inputStoreWithPartitions), outputs = Array(outputStoreWithPartitions))

    var taskPlan: BaseTaskPlan = null
    inputJob.name match {
      case RollPairService.aggregate => {
        taskPlan = new AggregateTaskPlan(new CommandURI(RollPairService.eggRunTaskCommand), taskPlanJob)
      }
      case RollPairService.map => {
        taskPlan = new MapTaskPlan(new CommandURI(RollPairService.eggRunTaskCommand), taskPlanJob)
      }
    }

    JobRunner.run(taskPlan)

    taskPlanJob
  }

  def join(inputJob: ErJob): ErJob = {
    val leftStore = inputJob.inputs.head
    val leftStoreWithPartitions = clusterManagerClient.getStore(leftStore)

    val rightStore = inputJob.inputs(1)
    val rightStoreWithPartitions = clusterManagerClient.getStore(rightStore)
    val isOutputSpecified = inputJob.outputs.nonEmpty

    //val outputLocator = leftLocator.copy(name = "testJoin")
    val outputStoreProposal = if (isOutputSpecified) {
      val specifiedOutput = inputJob.outputs.head
      if (specifiedOutput.partitions.isEmpty) {
        val outputTotalPartitions =
          if (StringUtils.equalsAny(RollPairService.reduce, RollPairService.aggregate)) 1
          else leftStoreWithPartitions.storeLocator.totalPartitions

        val outputStoreLocator = specifiedOutput.storeLocator.copy(totalPartitions = outputTotalPartitions)
        ErStore(storeLocator = outputStoreLocator)
      } else {
        specifiedOutput
      }
    } else {
      val outputStoreLocator = leftStore.storeLocator.fork()
      ErStore(storeLocator = outputStoreLocator.copy(totalPartitions = 1))
    }

    val outputStoreWithPartitions = clusterManagerClient.getOrCreateStore(outputStoreProposal)

    val taskPlanJob = inputJob.copy(inputs = Array(leftStoreWithPartitions, rightStoreWithPartitions), outputs = Array(outputStoreWithPartitions))
    val taskPlan = new JoinTaskPlan(new CommandURI(RollPairService.eggJoinCommand), taskPlanJob)
    scheduler.addPlan(taskPlan)

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }
}

object RollPairService {
  val clazz = classOf[RollPairService]
  val functorSerDes = DefaultScalaSerdes()

  val get = "get"
  val put = "put"
  val map = "map"
  val mapValues = "mapValues"
  val reduce = "reduce"
  val join = "join"
  val aggregate = "aggregate"
  val mapPartitions = "mapPartitions"
  val collapsePartitions = "collapsePartitions"
  val flatMap = "flatMap"
  val glom = "glom"
  val sample = "sample"
  val filter = "filter"
  val subtractByKey = "subtractByKey"
  val union = "union"
  val rollPair = "v1/roll-pair"
  val eggPair = "v1/egg-pair"

  val runTask = "runTask"
  val runJob = "runJob"
  val eggGetCommand = s"${eggPair}/${get}"
  val eggPutCommand = s"${eggPair}/${put}"
  val eggAggregateCommand = s"${eggPair}/${aggregate}"
  val eggMapCommand = s"${eggPair}/${map}"
  val eggMapValuesCommand = s"${eggPair}/${mapValues}"
  val eggReduceCommand = s"${eggPair}/${reduce}"
  val eggJoinCommand = s"${eggPair}/${join}"
  val eggMapPartitionsCommand = s"${eggPair}/${mapPartitions}"
  val eggCollapsePartitionsCommand = s"${eggPair}/${collapsePartitions}"
  val eggFlatMapCommand = s"${eggPair}/${flatMap}"
  val eggGlomMapCommand = s"${eggPair}/${glom}"
  val eggSampleMapCommand = s"${eggPair}/${sample}"
  val eggFilterMapCommand = s"${eggPair}/${filter}"
  val eggSubtractByKeyCommand = s"${eggPair}/${subtractByKey}"
  val eggUnionCommand = s"${eggPair}/${union}"

  val rollRunJobCommand = s"${rollPair}/${runJob}"
  val eggRunTaskCommand = s"${eggPair}/${runTask}"

  val rollGetCommand = s"${rollPair}/${get}"
  val rollPutCommand = s"${rollPair}/${put}"
  val rollAggregateCommand = s"${rollPair}/${aggregate}"
  val rollMapCommand = s"${rollPair}/${map}"
  val rollMapValuesCommand = s"${rollPair}/${mapValues}"
  val rollReduceCommand = s"${rollPair}/${reduce}"
  val rollJoinCommand = s"${rollPair}/${join}"
  val rollMapPartitionsCommand = s"${rollPair}/${mapPartitions}"
  val rollCollapsePartitionsCommand = s"${rollPair}/${collapsePartitions}"
  val rollFlatMapCommand = s"${rollPair}/${flatMap}"
  val rollGlomCommand = s"${rollPair}/${glom}"
  val rollSampleCommand = s"${rollPair}/${sample}"
  val rollFilterCommand = s"${rollPair}/${filter}"
  val rollSubtractByKeyCommand = s"${rollPair}/${subtractByKey}"
  val rollUnionCommand = s"${rollPair}/${union}"

  /*  CommandRouter.register(mapCommand,
      List(classOf[Array[Byte] => Array[Byte]]), clazz, "mapValues", null, null)*/
}