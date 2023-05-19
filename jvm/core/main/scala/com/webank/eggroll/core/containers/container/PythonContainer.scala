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

package com.webank.eggroll.core.containers.container

import java.nio.file.Path

class PythonContainer(
                       pythonExec: String,
                       scriptPath: String,
                       cwd: Path,
                       scriptArgs: Seq[String] = Seq.empty,
                       extraEnv: Map[String, String] = Map.empty,
                       stdErrFile: Option[Path] = None,
                       stdOutFile: Option[Path] = None,
                       workingDirectoryPreparer: Option[WorkingDirectoryPreparer] = None,
                       containerId: String,
                       processorId: Long
                     )

  extends ProcessContainer(
    command = Seq(pythonExec, "-u", scriptPath) ++ scriptArgs,
    extraEnv = extraEnv,
    stdOutFile = stdOutFile,
    stdErrFile = stdErrFile,
    cwd = cwd,
    workingDirectoryPreparer = workingDirectoryPreparer,
    containerId = containerId,
    processorId =  processorId
  )
