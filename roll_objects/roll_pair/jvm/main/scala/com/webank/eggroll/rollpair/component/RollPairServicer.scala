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

import com.webank.eggroll.core.client.ClusterManagerClient
import com.webank.eggroll.core.command.CommandURI
import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.schedule.{FilterTaskPlan, FlatMapTaskPlan, GlomTaskPlan, SampleTaskPlan, SubtractByKeyTaskPlan, UnionTaskPlan, _}
import com.webank.eggroll.core.serdes.DefaultScalaSerdes
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RollPairServicer() {
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
    val taskPlan = new MapTaskPlan(new CommandURI(RollPairServicer.eggMapValuesCommand), taskPlanJob)

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

    val taskPlan = new ShuffleTaskPlan(new CommandURI(RollPairServicer.eggMapCommand), job)
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

    val taskPlan = new ReduceTaskPlan(new CommandURI(RollPairServicer.eggReduceCommand), taskPlanJob)
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
    val taskPlan = new MapTaskPlan(new CommandURI(RollPairServicer.eggMapPartitionsCommand), taskPlanJob)

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
    val taskPlan = new MapTaskPlan(new CommandURI(RollPairServicer.eggCollapsePartitionsCommand), taskPlanJob)

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
    val taskPlan = new FlatMapTaskPlan(new CommandURI(RollPairServicer.eggFlatMapCommand), taskPlanJob)

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
    val taskPlan = new GlomTaskPlan(new CommandURI(RollPairServicer.eggGlomMapCommand), taskPlanJob)

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
    val taskPlan = new SampleTaskPlan(new CommandURI(RollPairServicer.eggSampleMapCommand), taskPlanJob)

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
    val taskPlan = new FilterTaskPlan(new CommandURI(RollPairServicer.eggFilterMapCommand), taskPlanJob)

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
          if (StringUtils.equalsAny(RollPairServicer.reduce, RollPairServicer.aggregate)) 1
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
    val taskPlan = new SubtractByKeyTaskPlan(new CommandURI(RollPairServicer.eggSubtractByKeyCommand), taskPlanJob)
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
          if (StringUtils.equalsAny(RollPairServicer.reduce, RollPairServicer.aggregate)) 1
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
    val taskPlan = new UnionTaskPlan(new CommandURI(RollPairServicer.eggUnionCommand), taskPlanJob)
    scheduler.addPlan(taskPlan)

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  // todo: compact all jobs to this call
  def runJob(inputJob: ErJob): ErJob = {
    val inputStore = inputJob.inputs.head

    val isOutputSpecified = inputJob.outputs.nonEmpty
    val finalInputs = ArrayBuffer[ErStore]()
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
      if (StringUtils.equalsAny(RollPairServicer.reduce, RollPairServicer.aggregate)) 1
      else finalInputTemplate.storeLocator.totalPartitions
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

    var taskPlan: BaseTaskPlan = null
    inputJob.name match {
      case RollPairServicer.aggregate => {
        taskPlan = new AggregateTaskPlan(new CommandURI(RollPairServicer.eggRunTaskCommand), taskPlanJob)
      }
      case RollPairServicer.map => {
        taskPlan = new MapTaskPlan(new CommandURI(RollPairServicer.eggRunTaskCommand), taskPlanJob)
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
          if (StringUtils.equalsAny(RollPairServicer.reduce, RollPairServicer.aggregate)) 1
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
    val taskPlan = new JoinTaskPlan(new CommandURI(RollPairServicer.eggJoinCommand), taskPlanJob)
    scheduler.addPlan(taskPlan)

    JobRunner.run(scheduler.getPlan())

    taskPlanJob
  }

  // todo: give default partition function: hash and mod
  def putBatch(inputJob: ErJob): ErJob = {
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

    val taskPlan = new ShuffleTaskPlan(new CommandURI(RollPairServicer.eggMapCommand), job)
    scheduler.addPlan(taskPlan)

    JobRunner.run(scheduler.getPlan())

    job
  }
}

object RollPairServicer {
  val clazz = classOf[RollPairServicer]
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
  val putBatch = "putBatch"
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
  val eggPutBatchCommand = s"${eggPair}/${putBatch}"

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
  val rollPutBatchCommand = s"${rollPair}/${putBatch}"

  /*  CommandRouter.register(mapCommand,
      List(classOf[Array[Byte] => Array[Byte]]), clazz, "mapValues", null, null)*/
}