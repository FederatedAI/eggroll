#  Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from ..core.command_model import CommandURI

DEFAULT_DELIM = '/'


def _to_service_name(prefix, method_name, delim=DEFAULT_DELIM):
    return f'{prefix}{delim}{method_name}'


def _create_command_uri(prefix, method_name):
    return CommandURI(_to_service_name(prefix, method_name))


class JobCommands:
    prefix = "v1/cluster-manager/job"

    SUBMIT_JOB = _create_command_uri(prefix, "submitJob")
    QUERY_JOB_STATUS = _create_command_uri(prefix, "queryJobStatus")
    QUERY_JOB = _create_command_uri(prefix, "queryJob")
    KILL_JOB = _create_command_uri(prefix, "killJob")
    DOWNLOAD_JOB = _create_command_uri(prefix, "downloadJob")


class ContainerCommands:
    prefix = "v1/node-manager/container"
    DOWNLOAD_CONTAINERS = _create_command_uri(prefix, "downloadContainers")
