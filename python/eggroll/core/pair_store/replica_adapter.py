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
from eggroll.core.pair_store.adapter import PairIterator, PairWriteBatch, \
    PairAdapter
from eggroll.core.pair_store.lmdb import LmdbAdapter, LmdbWriteBatch, \
    LmdbIterator
from eggroll.utils.log_utils import get_logger
from eggroll.core.pair_store.backup_adapter import BackupAdapter


L = get_logger()

# 64 * 1024 * 1024
LMDB_MAP_SIZE = 16 * 4_096 * 244_140    # follows storage-service-cxx's config here
LMDB_MAP_SIZE_WINDOWS_OS = 40 * 1024 * 1024
DEFAULT_DB = b'main'


class ReplicaWriteAdapter(PairAdapter):

    def __init__(self, options):
        self.firstAdapter = LmdbAdapter(options)
        self.secondAdapter = BackupAdapter(options)
        self.env = self.firstAdapter.env
        self.sub_db = self.firstAdapter.sub_db
        self.txn_w = self.firstAdapter.txn_w
        self.txn_r = self.firstAdapter.txn_r
        self.cursor = self.firstAdapter.cursor
        self.path = self.firstAdapter.path
        self.options = options

    def get(self, key):
        get_first = self.firstAdapter.get(key)
        get_second = self.secondAdapter.get(key)
        L.debug(f"rocksdb get data:{get_second}")
        return get_first

    def put(self, key, value):
        put_first = self.firstAdapter.put(key, value)
        put_second = self.secondAdapter.put(key, value)
        L.debug(f"rocksdb put data:{put_second}")
        return put_first

    def _init_write(self):
        if self.txn_w:
            return
        # with LmdbAdapter.env_lock:
        L.trace(f"lmdb init write={self.path}, path in options={self.options['path']}")
        self.txn_w = self.env.begin(db=self.sub_db, write=True)

    def _init_read(self):
        if self.txn_r:
            return
        # with LmdbAdapter.env_lock:
        L.trace(f"lmdb init read={self.path}, path in options={self.options['path']}")
        self.txn_r = self.env.begin(db=self.sub_db, write=False)
        self.cursor = self.txn_r.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    def __get_write_count(self):
        self._init_write()
        return self.txn_w.stat()['entries']

    def close(self):
        self.firstAdapter.close()

    def iteritems(self):
        return self.firstAdapter.iteritems()

    def new_batch(self):
        return self.firstAdapter.new_batch()

    def count(self):
        return self.firstAdapter.count()

    def delete(self, k):
        return self.firstAdapter.delete(k)

    def destroy(self, options: dict = None):
        self.firstAdapter.destroy(options)

    def is_sorted(self):
        return True


class ReplicaWriteIterator(PairIterator):
    def __init__(self, adapter: LmdbAdapter):
        self.firstIterator = LmdbIterator(adapter)
        self.cursor = self.firstIterator.cursor

    #seek for key, if key not exits then seek to nearby key
    def seek(self, key):
        return self.firstIterator.seek(key)

    # move cursor to the first key position
    # return True if success or False if db is empty
    def first(self):
        return self.firstIterator.first()

    # same as first() but last key position
    def last(self):
        return self.firstIterator.last()

    # return the current key
    def key(self):
        return self.firstIterator.key()

    def close(self):
        self.firstIterator.close()

    def __iter__(self):
        return self.cursor.__iter__()


class ReplicaWriteBatch(PairWriteBatch):
    def __init__(self, adapter: LmdbAdapter, txn):
        self.firstWriteBatch = LmdbWriteBatch(adapter, txn)

    def get(self, k):
        return self.firstWriteBatch.get(k)

    def put(self, k, v):
        return self.firstWriteBatch.put(k, v)

    def merge(self, merge_func, k, v):
        return self.firstWriteBatch.merge(merge_func, k, v)

    def delete(self, k):
        self.firstWriteBatch.delete(k)

    def write(self):
        pass

    def close(self):
        pass
