# -*- coding: utf-8 -*-
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

from eggroll.core.base_model import RpcMessage
from eggroll.core.proto import deepspeed_pb2
from eggroll.core.utils import _map_and_listify, _elements_to_proto, _stringify_dict
import typing
from eggroll.core.meta_model import ErProcessor


class DeepspeedJobMeta(RpcMessage):
    def __init__(self,
                 id='',
                 name: str = '',
                 job_type: str = '',
                 world_size: int = 0,
                 command_arguments: typing.Optional[typing.List[str]] = None,
                 environment_variables: typing.Optional[typing.Dict[str, str]] = None,
                 files: typing.Optional[typing.Dict[str, bytes]] = None,
                 zipped_files: typing.Optional[typing.Dict[str, bytes]] = None,
                 options: typing.Optional[typing.Dict] = None,
                 status: str = '',
                 processors: typing.Optional[list] = None):
        if command_arguments is None:
            command_arguments = []
        if environment_variables is None:
            environment_variables = {}
        if files is None:
            files = {}
        if zipped_files is None:
            zipped_files = {}
        if options is None:
            options = {}
        if processors is None:
            processors = []

        self._id = id
        self._name = name
        self._job_type = job_type
        self._world_size = world_size
        self._command_arguments = command_arguments
        self._environment_variables = environment_variables
        self._files = files
        self._zipped_files = zipped_files
        self._options = options
        self._status = status
        self._processors = processors

    def to_proto(self):
        return deepspeed_pb2.SubmitJobRequest(id=self._id,
                                              name=self._name,
                                              job_type=self._job_type,
                                              world_size=self._world_size,
                                              command_arguments=self._command_arguments,
                                              environment_variables=self._environment_variables,
                                              files=self._files,
                                              zipped_files=self._zipped_files,
                                              options=_stringify_dict(self._options),
                                              status=self._status,
                                              processors=_elements_to_proto(self._processors),
                                              )

    def to_proto_string(self):
        return self.to_proto().SerializeToString()

    @staticmethod
    def from_proto(pb_message):
        return DeepspeedJobMeta(id=pb_message.id,
                                name=pb_message.name,
                                job_type=pb_message.job_type,
                                world_size=pb_message.world_size,
                                command_arguments=pb_message.command_arguments,
                                environment_variables=pb_message.environment_variables,
                                files=pb_message.files,
                                zipped_files=pb_message.zipped_files,
                                options=dict(pb_message.options),
                                status=pb_message.status,
                                processors=_map_and_listify(ErProcessor.from_proto,
                                                            pb_message.processors),
                                )

    @staticmethod
    def from_proto_string(pb_string):
        pb_message = deepspeed_pb2.SubmitJobRequest()
        pb_message.ParseFromString(pb_string)
        return DeepspeedJobMeta.from_proto(pb_message)

    def __str__(self):
        return f'<ErSubmitJobMeta(' \
               f'id={self._id}, ' \
               f'name={self._name}, ' \
               f'job_type={self._job_type}, ' \
               f'command_arguments={self._command_arguments}, ' \
               f'environment_variables={self._environment_variables}, ' \
               f'files={self._files}, ' \
               f'zipped_files={self._zipped_files}, ' \
               f'options={self._options}, ' \
               f'status={self._status}, ' \
               f'processors=[***, len={len(self._processors)}]) ' \
               f'at {hex(id(self))}>'

    def __repr__(self):
        return self.__str__()
