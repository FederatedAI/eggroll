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

import com.webank.eggroll.core.command.CommandURI
import com.webank.eggroll.core.datastructure.TaskPlan
import com.webank.eggroll.core.meta.ErJob

abstract class BaseTaskPlan(_uri: CommandURI, _job: ErJob) extends TaskPlan {
  override def job: ErJob = _job

  override def uri: CommandURI = _uri
}

class MapTaskPlan(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)
class ReduceTaskPlan(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)
class ShuffleTaskPlan(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)
class JoinTaskPlan(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)
class AggregateTaskPlan(uri: CommandURI, job: ErJob) extends BaseTaskPlan(uri, job)

