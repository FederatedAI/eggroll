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
import gc
import os
import threading
import rocksdb

from eggroll.core.pair_store.adapter import PairWriteBatch, PairIterator, PairAdapter
from eggroll.utils.log_utils import get_logger
from eggroll.roll_pair.utils.pair_utils import get_data_dir

L = get_logger()


class RocksdbAdapter(PairAdapter):
    db_lock = threading.Lock()
    db_dict = dict()
    ref_dict = dict()
    count_dict = dict()
    _db_collected = False

    def __init__(self, options):
        """
        :param options:
          path: absolute local fs path
          create_if_missing: default true
        """
        with RocksdbAdapter.db_lock:
            super().__init__(options)
            self.path = options["path"]
            opts = rocksdb.Options()
            opts.create_if_missing = bool(options.get("create_if_missing", "True"))
            opts.compression = rocksdb.CompressionType.no_compression

            if self.path not in RocksdbAdapter.db_dict:
                if opts.create_if_missing:
                    os.makedirs(self.path, exist_ok=True)
                self.db = rocksdb.DB(self.path, opts)
                # self._dbref = rocksdb.weakref.ref(self.db, self._on_db_collected)
                L.info("path not in dict db path:{}".format(self.path))
                RocksdbAdapter.count_dict[self.path] = 0
                RocksdbAdapter.db_dict[self.path] = self.db
                # RocksdbAdapter.ref_dict = self._dbref
            else:
                L.info("path in dict:{}".format(self.path))
                self.db = RocksdbAdapter.db_dict[self.path]
                # self._dbref = RocksdbAdapter.ref_dict[self.path]
        RocksdbAdapter.count_dict[self.path] = RocksdbAdapter.count_dict[self.path] + 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # self.close()
        pass

    def __del__(self):
        # self.close()
        pass

    def _on_db_collected(self, ref):
        RocksdbAdapter._db_collected = True

    def ref_close(self):
        while not self._db_collected:
            try:
                del(self.db)
            except AttributeError:
                pass
            gc.collect()


    def get(self, key):
        return self.db.get(key)

    def put(self, key, value):
        self.db.put(key, value)

    def close(self):
        with RocksdbAdapter.db_lock:
            if hasattr(self, 'db') and self.path in RocksdbAdapter.db_dict:
                del RocksdbAdapter.db_dict[self.path]
                del self.db

    def close_forcefully(self):
        self.close()
        gc.collect()

    def iteritems(self):
        return RocksdbIterator(self)

    def new_batch(self):
        return RocksdbWriteBatch(self)

    def count(self):
        it = self.iteritems()
        return sum(1 for _ in it.it)

    def delete(self, k):
        self.db.delete(k)

    def destroy(self, options: dict = None):
        self.close()

        import shutil, os
        from pathlib import Path
        path = Path(self.path)
        store_path = path.parent
        real_data_dir = os.path.realpath(get_data_dir())

        if os.path.exists(store_path) \
            and not (store_path == "/"
                    or store_path == real_data_dir):
            try:
                shutil.rmtree(store_path)
                L.info(f'path: {store_path} has been destroyed')
            except FileNotFoundError:
                L.info(f'path: {store_path} has been destroyed by another partition')

    def is_sorted(self):
        return True


class RocksdbWriteBatch(PairWriteBatch):

    def __init__(self, adapter: RocksdbAdapter, chunk_size=100_000):
        self.chunk_size = chunk_size
        self.batch = rocksdb.WriteBatch()
        self.adapter = adapter
        self.write_count = 0
        self.manual_merger = dict()
        self.has_write_op = False

    def get(self, k):
        raise NotImplementedError

    def put(self, k, v):
        if len(self.manual_merger) == 0:
            self.has_write_op = True
            self.batch.put(k, v)
            self.write_count += 1
            if self.write_count % self.chunk_size == 0:
                self.write()
        else:
            self.manual_merger[k] = v

    def merge(self, merge_func, k, v):
        if self.has_write_op:
            self.write()

        if k in self.manual_merger:
            self.manual_merger[k] = merge_func(self.manual_merger[k], v)
        else:
            if not self.has_write_op:
                self.manual_merger[k] = v
            else:
                old_value = self.adapter.get(k)
                if old_value is None:
                    self.manual_merger[k] = v
                else:
                    self.manual_merger[k] = merge_func(old_value, v)
        if len(self.manual_merger) >= self.chunk_size:
            self.write_merged()

    def delete(self, k):
        self.batch.delete(k)

    def write(self):
        if self.batch.count() > 0:
            self.adapter.db.write(self.batch)
            self.batch.clear()

    def write_merged(self):
        for k, v in sorted(self.manual_merger.items(), key=lambda kv: kv[0]):
            self.batch.put(k, v)
            self.write_count += 1
        self.has_write_op = True
        self.manual_merger.clear()
        self.write()

    def close(self):
        if self.batch:
            if self.manual_merger is not None:
                self.write_merged()
            self.write()
            del self.batch
            self.batch = None


class RocksdbIterator(PairIterator):
    def __init__(self, adapter: RocksdbAdapter):
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
