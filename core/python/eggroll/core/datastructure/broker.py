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

from queue import Queue
from eggroll.utils import log_utils
import time

log_utils.setDirectory()
LOGGER = log_utils.getLogger()

class Broker(object):
  def is_write_finished(self):
    raise NotImplementedError()

  def signal_write_finish(self):
    raise NotImplementedError()

  def get_remaining_write_signal_count(self):
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

  def drain_to(self, target, max_elements = 10000):
    raise NotImplementedError()


class FifoBroker(Broker):
  __broker_seq = 0
  #todo: make maxsize configurable
  def __init__(self, max_capacity = 10000, write_signals = 1, name = f"fifobroker-{time.time()}-{__broker_seq}"):
    FifoBroker.__broker_seq += 1
    self.__queue = Queue(maxsize=max_capacity)
    self.__write_finished = False
    self.__history_total = 0
    self.__history_none_total = 0
    self.__remaining_write_signal_count = write_signals
    self.__total_write_signals = write_signals
    self.__max_capacity = max_capacity

  def get_total_write_signals(self):
    return self.__total_write_signals

  def is_write_finished(self):
    return self.__write_finished

  def signal_write_finish(self):
    if self.is_write_finished():
      raise ValueError(f"finish signaling overflows. initial value: {self.__total_write_signals}")

    self.__remaining_write_signal_count -= 1
    if self.__remaining_write_signal_count <= 0:
      self.__write_finished = True

  def get_remaining_write_signal_count(self):
    return self.__remaining_write_signal_count

  def is_read_ready(self):
    return not self.__queue.empty()

  def is_closable(self):
    result = self.is_write_finished() and not self.is_read_ready()
    return result

  def total(self):
    return self.__history_total

  def total_none(self):
    return self.__history_none_total

  def size(self):
    return self.__queue.qsize()

  def put_nowait(self, item):
    return self.put(item=item, block=False)

  def get_nowait(self):
    return self.get(block=False)

  def put(self, item, block=True, timeout=None):
    return self.__queue.put(item=item, block=block, timeout=timeout)

  def get(self, block=True, timeout=None):
    return self.__queue.get(block=True, timeout=timeout)

  def drain_to(self, target, max_elements = 10000):
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
    if not self.is_closable():
      return self.get(block=True)
    else:
      raise StopIteration

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    pass