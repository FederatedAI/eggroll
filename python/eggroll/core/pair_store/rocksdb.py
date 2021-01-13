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
import logging
import os
import threading
from collections import defaultdict

import shutil
from pathlib import Path
import time

from eggroll.core.conf_keys import RollPairConfKeys
from eggroll.core.pair_store.adapter import PairWriteBatch, PairIterator, PairAdapter
from eggroll.utils.log_utils import get_logger
from eggroll.roll_pair.utils.pair_utils import get_data_dir

L = get_logger()

try:
    import rocksdb
except ModuleNotFoundError:
    L.info(f'python-rocksdb not installed')


class RocksdbAdapter(PairAdapter):
    db_dict = dict()
    count_dict = dict()
    lock_dict = defaultdict(threading.Lock)
    db_lock = threading.Lock()

    @staticmethod
    def release_db_resource():
        for path, db in RocksdbAdapter.db_dict.items():
            del db

    def __init__(self, options):
        """
        :param options:
          path: absolute local fs path
          create_if_missing: default true
        """
        self.path = options["path"].strip()
        with RocksdbAdapter.db_lock:
            super().__init__(options)

            L.debug(f'initing db={self.path}, db_dict={RocksdbAdapter.db_dict}')
            self.is_closed = False

            if self.path not in RocksdbAdapter.db_dict:
                opts = rocksdb.Options()
                opts.create_if_missing = (str(options.get("create_if_missing", "True")).lower() == 'true')
                opts.compression = rocksdb.CompressionType.no_compression
                # todo:0: parameterize write_buffer_size
                opts.max_open_files = -1
                # opts.allow_concurrent_memtable_write = True
                opts.write_buffer_size = 128 * 1024
                opts.max_write_buffer_number = 1
                opts.allow_mmap_writes = False
                opts.allow_mmap_reads = False
                opts.arena_block_size = 1024
                opts.allow_concurrent_memtable_write = True
                opts.max_bytes_for_level_base = 1 << 20
                opts.target_file_size_base = 1 << 22
                opts.num_levels = 1
                opts.level0_slowdown_writes_trigger = 1
                opts.table_cache_numshardbits = 1
                opts.manifest_preallocation_size = 128 * 1024
                opts.table_factory = rocksdb.BlockBasedTableFactory(no_block_cache=True, block_size=128*1024)

                if opts.create_if_missing:
                    os.makedirs(self.path, exist_ok=True)
                self.db = None
                # todo:0: parameterize max_retry_cnt
                max_retry_cnt = 30
                retry_cnt = 0
                while not self.db:
                    try:
                        self.db = rocksdb.DB(self.path, opts)
                    except rocksdb.errors.RocksIOError as e:
                        if retry_cnt > max_retry_cnt:
                            L.exception(f'failed to open path={self.path} after retry.')
                            raise e
                        retry_cnt += 1
                        L.warn(f'fail to open db path={self.path}. retry_cnt={retry_cnt}. db_dict={RocksdbAdapter.db_dict}')
                        gc.collect()
                        time.sleep(1)
                L.trace(f'RocksdbAdapter.__init__: path not in dict db path={self.path}')
                RocksdbAdapter.db_dict[self.path] = self.db
                RocksdbAdapter.count_dict[self.path] = 0
            else:
                L.trace(f'RocksdbAdapter.__init__: path in dict={self.path}')
                self.db = RocksdbAdapter.db_dict[self.path]
            prev_count = RocksdbAdapter.count_dict[self.path]
            RocksdbAdapter.count_dict[self.path] = prev_count + 1
            L.trace(f"RocksdbAdapter.__init__: path={self.path}, prev_count={prev_count}, cur_count={prev_count + 1}")

    def __enter__(self):
        if self.is_closed:
            raise RuntimeError(f"cannot Open Same adapter={self}, path={self.path} twice")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.is_closed:
            L.warning(f"db={self.path} has been closed")
            return
        self.close()

    def __del__(self):
        pass

    def get(self, key):
        ret = None
        try:
            ret = self.db.get(key)
        except:
            raise ValueError(f"get k={key} raise Error")
        return ret

    def put(self, key, value):
        ret = None
        try:
            ret = self.db.put(key, value)
        except:
            raise ValueError(f"put k={key}, v={value} raise Error")
        return ret

    def close(self):
        if L.isEnabledFor(logging.TRACE):
            L.trace(f'RocksdbAdapter.close for path={self.path}. is_closed={self.is_closed}, db_dict={RocksdbAdapter.db_dict}')
        else:
            L.debug(f'RocksdbAdapter.close for path={self.path}. is_closed={self.is_closed}')
        with RocksdbAdapter.db_lock:
            if self.is_closed:
                return
            count = RocksdbAdapter.count_dict.get(self.path, None)
            if not count or count - 1 <= 0:
                L.debug(f'RocksdbAdapter.close: actually closing path={self.path}. count={count}')
                del RocksdbAdapter.db_dict[self.path]
                del RocksdbAdapter.count_dict[self.path]
                self.is_closed = True
            else:
                count -= 1
                RocksdbAdapter.count_dict[self.path] = count
                L.trace(f'RocksdbAdapter.close: ref count -= 1 for path={self.path}. current ref count={count}')
            del self.db
            gc.collect()
        if L.isEnabledFor(logging.TRACE):
            L.trace(f'RocksdbAdapter.closed: path={self.path}, is_closed={self.is_closed}, db_dict={RocksdbAdapter.db_dict}')
        else:
            L.debug(f'RocksdbAdapter.closed: path={self.path}, is_closed={self.is_closed}')

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

        path = Path(self.path)
        store_path = path.parent
        real_data_dir = os.path.realpath(get_data_dir())

        if os.path.exists(path) \
                and not (path == "/"
                         or store_path == real_data_dir):
            try:
                shutil.rmtree(path, ignore_errors=False)
                L.trace(f'path={path} has been destroyed')
                os.rmdir(store_path)
            except FileNotFoundError as e:
                L.trace(f'path={path} has been destroyed by another partition')
            except OSError as e:
                if e.args[0] != 66 and e.args[0] != 39:
                    raise e

    def is_sorted(self):
        return True


class RocksdbWriteBatch(PairWriteBatch):

    def __init__(self, adapter: RocksdbAdapter):
        self.batch_size = RollPairConfKeys.EGGROLL_ROLLPAIR_ROCKSDB_WRITEBATCH_SIZE.get()
        self.batch = rocksdb.WriteBatch()
        self.adapter = adapter
        self.write_count = 0
        self.manual_merger = dict()
        self.has_write_op = False
        L.trace(f"created writeBatch={self.adapter.path} batch_size={self.batch_size}")

    def get(self, k):
        raise NotImplementedError

    def put(self, k, v):
        if len(self.manual_merger) == 0:
            self.has_write_op = True
            self.batch.put(k, v)
            self.write_count += 1
            if self.write_count % self.batch_size == 0:
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
        if len(self.manual_merger) >= self.batch_size:
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
        L.trace(f"created Iterator={self.adapter.path}")

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

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self.adapter
        del self.it

    def __iter__(self):
        return self.it
