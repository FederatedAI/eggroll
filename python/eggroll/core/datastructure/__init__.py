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

from importlib import import_module
from concurrent.futures import _base, ThreadPoolExecutor
from eggroll.core.datastructure.threadpool import ErThreadUnpooledExecutor
from eggroll.core.datastructure.queue import _PySimpleQueue
from eggroll.utils.log_utils import get_logger

L = get_logger()

try:
    from queue import SimpleQueue
except ImportError:
    SimpleQueue = _PySimpleQueue


def create_executor_pool(canonical_name: str = None, max_workers=None, thread_name_prefix=None, *args, **kwargs) -> _base.Executor:
    if not canonical_name:
        canonical_name = "concurrent.futures.ThreadPoolExecutor"
    module_name, class_name = canonical_name.rsplit(".", 1)
    _module = import_module(module_name)
    _class = getattr(_module, class_name)

    return _class(max_workers=max_workers, thread_name_prefix=thread_name_prefix, *args, **kwargs)


def create_simple_queue(*args, **kwargs):
    return SimpleQueue()
