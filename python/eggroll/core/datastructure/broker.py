# -*- coding: UTF-8 -*-
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
import queue
import time
from queue import Queue

from eggroll.core.conf_keys import CoreConfKeys
from eggroll.utils.log_utils import get_logger
from threading import Lock

L = get_logger()


class Broker(object):
    def is_write_finished(self):
        raise NotImplementedError()

    def signal_write_finish(self):
        raise NotImplementedError()

    def get_active_writers_count(self):
        raise NotImplementedError()

    def is_read_ready(self):
        raise NotImplementedError()

    def is_closable(self):
        raise NotImplementedError()

    def total(self):
        raise NotImplementedError()

    def total_none(self):
        raise NotImplementedError()

    def size(self):
        raise NotImplementedError()

    def put_nowait(self, item):
        raise NotImplementedError()

    def get_nowait(self):
        raise NotImplementedError()

    def put(self, item, block=True, timeout=None):
        raise NotImplementedError()

    def get(self, block=True, timeout=None):
        raise NotImplementedError()

    def drain_to(self, target, max_elements=10000):
        raise NotImplementedError()


class BrokerClosed(Exception):
    'Exception raised by eggroll Broker'
    pass


class FifoBroker(Broker):
    __broker_seq = 0

    # todo:1: make maxsize configurable
    def __init__(self,
            maxsize=CoreConfKeys.EGGROLL_CORE_FIFOBROKER_DEFAULT_SIZE.default_value,
            writers=1,
            name=f"fifobroker-{time.time()}-{__broker_seq}"):
        FifoBroker.__broker_seq += 1
        self.__max_capacity = maxsize
        self.__queue = Queue(maxsize=maxsize)
        self.__active_writers = writers
        self.__total_writers = writers
        self.__active_writers_lock = Lock()
        self.__name = name

    def get_total_writers(self):
        return self.__total_writers

    def is_write_finished(self):
        with self.__active_writers_lock:
            return self.__active_writers <= 0

    def signal_write_finish(self):
        if self.is_write_finished():
            raise ValueError(
                f"finish signaling overflows. initial value: {self.__total_writers}")
        else:
            with self.__active_writers_lock:
                self.__active_writers -= 1

    def get_active_writers_count(self):
        with self.__active_writers_lock:
            return self.__active_writers

    def is_read_ready(self):
        return not self.__queue.empty()

    def is_closable(self):
        return self.is_write_finished() and self.__queue.empty()

    def size(self):
        return self.__queue.qsize()

    def put_nowait(self, item):
        return self.put(item=item, block=False)

    def get_nowait(self):
        return self.get(block=False)

    def put(self, item, block=True, timeout=None):
        if self.is_write_finished():
            raise ValueError(f'Broker write finished. id={hex(id(self))}')

        self.__queue.put(item=item, block=block, timeout=timeout)

    def get(self, block=True, timeout=None):
        if self.is_closable():
            raise BrokerClosed

        return self.__queue.get(block=block, timeout=timeout)

    def drain_to(self, target, max_elements=10000):
        if hasattr(target, 'append'):
            func = getattr(target, 'append')
        elif hasattr(target, 'put'):
            func = getattr(target, 'put')

        cur_count = 0
        while self.is_read_ready() and cur_count < max_elements:
            func(self.get())
            cur_count += 1

    def __iter__(self):
        return self

    def __next__(self):
        while not self.is_closable():
            try:
                return self.get(block=True, timeout=0.1)
            except queue.Empty as e:
                # retry
                pass
            except BrokerClosed as e:
                raise StopIteration
        raise StopIteration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
