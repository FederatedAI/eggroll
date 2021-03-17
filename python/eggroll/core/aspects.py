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
#
#

import inspect
from time import time, perf_counter

from eggroll.utils.log_utils import get_logger

L = get_logger(filename='profile')


def _method_profile_logger(func):
    def wrapper(*args, **kwargs):
        start_wall_time = time()
        start_cpu_time = perf_counter()

        result = func(*args, **kwargs)

        end_wall_time = time()
        end_cpu_time = perf_counter()

        code = func.__code__
        try:
            outerframes = inspect.getouterframes(inspect.currentframe(), 2)
            real_caller = outerframes[1]
            L.trace(f'{{"metric_type": "func_profile", '
                   f'"qualname": "{func.__qualname__}", '
                   f'"caller": "{real_caller.filename.rsplit("/", 1)[-1]}:{real_caller.lineno}", '
                   f'"cpu_time": {end_cpu_time - start_cpu_time}, '
                   f'"wall_time": {end_wall_time - start_wall_time}}}')

            return result
        except Exception as e:
            L.trace(f'{{"metric_type": "func_profile", '
                   f'"qualname": "{func.__qualname__}", '
                   f'"caller": "unknown", '
                   f'"cpu_time": {end_cpu_time - start_cpu_time}, '
                   f'"wall_time": {end_wall_time - start_wall_time}}}')
    return wrapper
