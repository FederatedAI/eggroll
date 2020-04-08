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

import json
from time import sleep

import psutil

from eggroll.utils.log_utils import get_logger

L = get_logger(filename='profile')


def get_system_metric(interval=1):
    io_start = psutil.disk_io_counters()._asdict()
    net_start = psutil.net_io_counters()._asdict()
    cpu_percent = psutil.cpu_percent(interval=interval)
    memory = psutil.virtual_memory()._asdict()
    io_end = psutil.disk_io_counters()._asdict()
    net_end = psutil.net_io_counters()._asdict()

    result = {}

    io = {}
    for k, v in io_end.items():
        io[k] = io_end[k] - io_start[k]

    net = {}
    for k, v in net_end.items():
        net[k] = net_end[k] - net_start[k]

    result['cpu_percent'] = cpu_percent
    result['memory'] = memory
    result['io'] = io
    result['net'] = net
    result['metric_type'] = 'system'

    return result


if __name__ == "__main__":
    while True:
        system_metric = get_system_metric()
        msg = json.dumps(system_metric)
        L.info(msg)
        sleep(10)
