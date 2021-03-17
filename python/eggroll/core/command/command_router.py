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

import logging
import time
from importlib import import_module

from eggroll.core.meta_model import ErTask
from eggroll.core.proto import meta_pb2
from eggroll.utils.log_utils import get_logger

L = get_logger()


class CommandRouter(object):
    __instance = None

    @staticmethod
    def get_instance():
        if not CommandRouter.__instance:
            CommandRouter()
        return CommandRouter.__instance

    def __init__(self):
        if CommandRouter.__instance:
            raise Exception(
                    'CommandRouter a singleton class and the instance has been initialized')
        else:
            CommandRouter.__instance = self
            self._service_route_table = dict()  # key: service_name, value: (instance, class, method)

    def register(self,
            service_name: str,
            service_param_deserializers: list = None,
            service_result_serializers: list = None,
            route_to_module_name: str = '',
            route_to_class_name: str = '',
            route_to_method_name: str = '',
            route_to_call_based_class_instance=None,
            call_based_class_instance_init_arg=None):
        if service_param_deserializers is None:
            service_param_deserializers = []
        if service_result_serializers is None:
            service_result_serializers = []
        if service_name in self._service_route_table:
            raise ValueError(
                    f'service {service_name} has already been registered at ${self._service_route_table[service_name]}')

        # todo:2: consider scope. now default to a 'prototype' style and no init args
        _module = import_module(route_to_module_name)
        _class = getattr(_module, route_to_class_name)
        _method = getattr(_class, route_to_method_name)

        self._service_route_table[service_name] = (None, _class, _method)
        L.info("service:{} has registered".format(service_name))

    def dispatch(self, service_name: str, args, kwargs):
        if service_name not in self._service_route_table:
            raise ValueError(f'{service_name} has not been registered yet')

        _instance, _class, _method = self._service_route_table[service_name]

        if not _instance:
            _instance = _class()

        task_name = ''
        deserialized_args = list()
        for arg in args:
            task = meta_pb2.Task()
            msg_len = task.ParseFromString(arg)
            deserialized_task = ErTask.from_proto(task)
            if not task_name:
                task_name = deserialized_task._name
            deserialized_args.append(deserialized_task)

        L.debug(f"[CS] calling: [{service_name}], task_name={task_name}, request={deserialized_args}, len={len(args)}")

        start = time.time()
        try:
            call_result = _method(_instance, *deserialized_args)
        except Exception as e:
            L.exception(f'Failed to dispatch to [{service_name}], task_name: {task_name}, request: {deserialized_args}')
            raise e
        elapsed = time.time() - start
        if L.isEnabledFor(logging.TRACE):
            L.trace(f"[CS] called (elapsed={elapsed}): [{service_name}]: task_name={task_name}, request={deserialized_args}, result={call_result}")
        else:
            L.debug(f"[CS] called (elapsed={elapsed}): [{service_name}], task_name={task_name}, request={deserialized_args}")

        # todo:2: defaulting to pb message. need changes when other types of result is present
        return [call_result.to_proto().SerializeToString()]

    def query(self, service_name: str):
        return self._service_route_table[service_name]
