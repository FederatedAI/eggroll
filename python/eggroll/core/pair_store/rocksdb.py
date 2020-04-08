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
import os
import threading

import rocksdb

from eggroll.utils.log_utils import get_logger

L = get_logger()

from eggroll.core.pair_store.adapter import PairWriteBatch, PairIterator, PairAdapter

class RocksdbWriteBatch(PairWriteBatch):

    def __init__(self, adapter, chunk_size=100000):
        self.chunk_size = chunk_size
        self.batch = rocksdb.WriteBatch()
        self.adapter = adapter
        self.key = None
        self.value = None
        self.write_count = 0

    def put(self, k, v):
        self.key = k
        self.value = v
        self.batch.put(k, v)
        self.write_count += 1
        if self.write_count >= 100000:
            self.write()

    def delete(self, k):
        self.adapter.db.delete(k)

    def write(self):
        self.adapter.db.write(self.batch)
        self.batch.clear()

    def close(self):
        if self.batch:
            self.write()
            del self.batch
            self.batch = None

class RocksdbIterator(PairIterator):
    def __init__(self, adapter):
        self.adapter = adapter
        self.it = adapter.db.iteritems()
        self.it.seek_to_first()

    def first(self):
        count = 0
        self.it.seek_to_first()
        for k, v in self.it:
            count += 1
            break
        self.it.seek_to_first()
        return (count != 0)

    def last(self):
        count = 0
        self.it.seek_to_last()
        for k, v in self.it:
            count += 1
            break
        self.it.seek_to_last()
        return (count != 0)

    def seek(self, key):
        self.it.seek(key)

    def key(self):
        return self.it.get()[0]

    def close(self):
        pass

    def __iter__(self):
        return self.it


class RocksdbAdapter(PairAdapter):
    env_lock = threading.Lock()
    env_dict = dict()
    count_dict = dict()

    def __init__(self, options):
        """
        :param options:
          path: absolute local fs path
          create_if_missing: default true
        """
        with RocksdbAdapter.env_lock:
            super().__init__(options)
            self.path = options["path"]
            opts = rocksdb.Options()
            opts.create_if_missing = bool(options.get("create_if_missing", "True"))
            opts.compression = rocksdb.CompressionType.no_compression
            if self.path not in RocksdbAdapter.env_dict:
                if opts.create_if_missing:
                    os.makedirs(self.path, exist_ok=True)
                self.db = rocksdb.DB(self.path, opts)
                L.info("path not in dict db path:{}".format(self.path))
                RocksdbAdapter.count_dict[self.path] = 0
                RocksdbAdapter.env_dict[self.path] = self.db
            else:
                L.info("path in dict:{}".format(self.path))
                self.db = RocksdbAdapter.env_dict[self.path]
        RocksdbAdapter.count_dict[self.path] = RocksdbAdapter.count_dict[self.path] + 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    def get(self, key):
        return self.db.get(key)

    def put(self, key, value):
        self.db.put(key, value)

    def close(self):
        with RocksdbAdapter.env_lock:
            if hasattr(self, 'db'):
                count = RocksdbAdapter.count_dict[self.path]
                if not count or count - 1 <= 0:
                    del RocksdbAdapter.env_dict[self.path]
                    del RocksdbAdapter.count_dict[self.path]
                else:
                    RocksdbAdapter.count_dict[self.path] = count - 1
        del self.db

    def iteritems(self):
        return RocksdbIterator(self)

    def new_batch(self):
        return RocksdbWriteBatch(self)

    def count(self):
        it = self.iteritems()
        return sum(1 for _ in it.it)

    def delete(self, k):
        self.db.delete(k)

    def destroy(self):
        self.close()
        import shutil, os
        from pathlib import Path
        shutil.rmtree(self.path)
        path = Path(self.path)
        try:
            if not os.listdir(path.parent):
                os.removedirs(path.parent)
                L.debug("finish destroy, path:{}".format(self.path))
        except:
            L.info("path :{} has destroyed".format(self.path))

    def is_sorted(self):
        return True
