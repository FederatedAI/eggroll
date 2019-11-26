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

from eggroll.core.io.kv_adapter import SortedKvAdapter, SortedKvIterator, SortedKvWriteBatch
from eggroll.core.io.format import BinRowRollPairBatchReader, BinRowRollPairBatchWriter
from eggroll.core.transfer.transfer_service import GrpcTransferServicer


class BrokerRowRollPairAdapter(SortedKvAdapter):
  def __init__(self, options = {}):
    self._options = options
    self._broker = options['broker']
    self._writer = BinRowRollPairBatchWriter()
    self._reader = BinRowRollPairBatchReader()

  def close(self):
    raise NotImplementedError()

  def iteritems(self):
    return BrokerRowRollPairIterator()

  def new_batch(self):
    raise NotImplementedError()

  def get(self, key):
    raise NotImplementedError()

  def put(self, key, value):
    raise NotImplementedError()

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()

class BrokerRowRollPairIterator(SortedKvIterator):
  def close(self):
    raise NotImplementedError()

  def __iter__(self):
    raise NotImplementedError()

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()


class BrokerRowRollPairWriteBatch(SortedKvWriteBatch):
  def put(self, k, v):
    raise NotImplementedError()

  def write(self):
    raise NotImplementedError()

  def close(self):
    raise NotImplementedError()

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()