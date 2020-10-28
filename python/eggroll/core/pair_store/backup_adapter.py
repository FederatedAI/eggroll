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

from eggroll.core.pair_store.adapter import PairWriteBatch, PairIterator, PairAdapter
from eggroll.core.pair_store.rocksdb import RocksdbAdapter, RocksdbIterator, RocksdbWriteBatch
from eggroll.utils.log_utils import get_logger

L = get_logger()


class BackupAdapter(PairAdapter):
    def __init__(self, options):
        self.backupAdapter = RocksdbAdapter(options)
        self.options = options

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        pass

    def get(self, key):
        return self.backupAdapter.get(key)

    def put(self, key, value):
        return self.backupAdapter.put(key, value)

    def close(self):
        self.backupAdapter.close()

    def iteritems(self):
        return self.backupAdapter.iteritems()

    def new_batch(self):
        return self.backupAdapter.new_batch()

    def count(self):
        return self.backupAdapter.count()

    def delete(self, k):
        return self.backupAdapter.delete(k)

    def destroy(self, options: dict = None):
        self.backupAdapter.destroy(options)

    def is_sorted(self):
        return True


class BackupWriteBatch(PairWriteBatch):
    def __init__(self, adapter: RocksdbAdapter):
        self.backupWriteBatch = RocksdbWriteBatch(adapter)
        self.adapter = self.backupWriteBatch.adapter

    def get(self, k):
        return self.backupWriteBatch.get(k)

    def put(self, k, v):
        return self.backupWriteBatch.put(k, v)

    def merge(self, merge_func, k, v):
        return self.backupWriteBatch.merge(merge_func, k, v)

    def delete(self, k):
        self.backupWriteBatch.delete(k)

    def write(self):
        self.backupWriteBatch.write()

    def write_merged(self):
        self.backupWriteBatch.write_merged()

    def close(self):
        self.backupWriteBatch.close()


class BackupIterator(PairIterator):
    def __init__(self, adapter: RocksdbAdapter):
        self.backupIterator = RocksdbIterator(adapter)
        self.adapter = self.backupIterator.adapter

    def first(self):
        return self.backupIterator.first()

    def last(self):
        return self.backupIterator.last()

    def seek(self, key):
        return self.backupIterator.seek(key)

    def key(self):
        return self.backupIterator.key()

    def close(self):
        pass

    def __iter__(self):
        return self.backupIterator.it


