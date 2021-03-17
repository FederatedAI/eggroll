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
import platform
import sys
import threading

import lmdb

from eggroll.core.pair_store.adapter import PairIterator, PairWriteBatch, \
    PairAdapter
from eggroll.utils.log_utils import get_logger

L = get_logger()

#64 * 1024 * 1024
LMDB_MAP_SIZE = 16 * 4_096 * 244_140    # follows storage-service-cxx's config here
LMDB_MAP_SIZE_WINDOWS_OS = 40 * 1024 * 1024
DEFAULT_DB = b'main'


class LmdbAdapter(PairAdapter):
    env_lock = threading.Lock()
    env_dict = dict()
    count_dict = dict()
    sub_db_dict = dict()
    # txn_w_dict = dict()

    def get(self, key):
        self._init_read()
        return self.cursor.get(key)

    def put(self, key, value):
        self._init_write()
        ret = None
        try:
            ret = self.txn_w.put(key, value)
        except lmdb.BadValsizeError as e:
            L.info(f"key={key}, value={value} raise lmdb.BadValsizeError")
        return ret

    def __init__(self, options):
        self.env = None
        self.sub_db = None
        self.txn_w = None
        self.txn_r = None
        self.cursor = None
        self.options = options
        with LmdbAdapter.env_lock:
            super().__init__(options)
            self.path = options["path"]
            lmdb_map_size = options.get("lmdb_map_size", LMDB_MAP_SIZE if platform.system() != 'Windows' else LMDB_MAP_SIZE_WINDOWS_OS)
            create_if_missing = (str(options.get("create_if_missing", "True")).lower() == 'true')
            if self.path not in LmdbAdapter.env_dict:
                if create_if_missing:
                    os.makedirs(self.path, exist_ok=True)
                L.debug("lmdb init create env={}".format(self.path))
                writemap = False if platform.system() == 'Darwin' else True
                if not os.path.exists(self.path):
                    os.makedirs(self.path, exist_ok=True)
                self.env = lmdb.open(self.path,
                                     create=create_if_missing,
                                     max_dbs=128,
                                     sync=False,
                                     map_size=lmdb_map_size,
                                     writemap=writemap,
                                     lock=False)
                self.sub_db = self.env.open_db(DEFAULT_DB)
                try:
                    L.trace(f"LmdbAdapter.init: env={self.path}, data count={self.count()}")
                except Exception as e:
                    L.exception(f"LmdbAdapter.init: fail to get data count of env={self.path}")
                LmdbAdapter.count_dict[self.path] = 0
                LmdbAdapter.env_dict[self.path] = self.env
                LmdbAdapter.sub_db_dict[self.path] = self.sub_db
            else:
                L.trace("lmdb init get env={}".format(self.path))
                self.env = LmdbAdapter.env_dict[self.path]
                self.sub_db = LmdbAdapter.sub_db_dict[self.path]
            LmdbAdapter.count_dict[self.path] = LmdbAdapter.count_dict[self.path] + 1
            L.trace(f"lmdb inited={self.path}")

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
        with LmdbAdapter.env_lock:
            if not self.env:
                return
            if self.txn_r:
                self.txn_r.commit()
                self.cursor.close()
            if self.txn_w:
                try:
                    L.trace(f"LmdbAdapter.close: env={self.path} data count={self.__get_write_count()}")
                except Exception as e:
                    L.exception(f"LmdbAdapter.close: fail to get data count of env={self.path}")
                self.txn_w.commit()
            if self.env:
                count = LmdbAdapter.count_dict[self.path]
                if not count or count - 1 <= 0:
                    L.debug(f"LmdbAdapter: actually closing {self.path}")
                    try:
                        if "EGGROLL_LMDB_ENV_CLOSE_ENABLE" in os.environ \
                                and os.environ["EGGROLL_LMDB_ENV_CLOSE_ENABLE"] == '1'\
                                or sys.platform == 'win32':
                            self.env.close()
                            L.debug(f"EGGROLL_LMDB_ENV_CLOSE_ENABLE is True, finish close lmdb env obj: {self.path}")
                        else:
                            L.trace(f"lmdb env={self.path} not close while closing LmdbAdapter")
                    except:
                        L.warning(f"txn commit or cursor, env={self.path} have closed before")

                    del LmdbAdapter.env_dict[self.path]
                    del LmdbAdapter.sub_db_dict[self.path]
                    del LmdbAdapter.count_dict[self.path]
                else:
                    LmdbAdapter.count_dict[self.path] = count - 1
                self.env = None

    def iteritems(self):
        return LmdbIterator(self)

    def new_batch(self):
        self._init_write()
        return LmdbWriteBatch(self, self.txn_w)

    def count(self):
        self._init_read()
        return self.txn_r.stat()["entries"]

    def delete(self, k):
        self._init_write()
        return self.txn_w.delete(k)

    def destroy(self, options: dict = None):
        self.close()
        import shutil, os
        from pathlib import Path

        try:
            shutil.rmtree(self.path)
            path = Path(self.path)
            if not os.listdir(path.parent):
                os.removedirs(path.parent)
                L.trace("finish destroy, path={}".format(self.path))
        except:
            L.debug("path={} has destroyed".format(self.path))

    def is_sorted(self):
        return True


class LmdbIterator(PairIterator):
    def __init__(self, adapter: LmdbAdapter):
        L.trace(f"creating lmdb iterator of env={adapter.path}")
        self.adapter = adapter
        # with LmdbAdapter.env_lock:
        self.txn_r = adapter.env.begin(db=adapter.sub_db, write=False)
        self.cursor = self.txn_r.cursor()
        L.trace(f"created lmdb iterator of env={adapter.path}")

    #seek for key, if key not exits then seek to nearby key
    def seek(self, key):
        return self.cursor.set_range(key)

    # move cursor to the first key position
    # return True if success or False if db is empty
    def first(self):
        return self.cursor.first()

    # same as first() but last key position
    def last(self):
        return self.cursor.last()

    # return the current key
    def key(self):
        return self.cursor.key()

    def close(self):
        self.cursor.close()
        self.txn_r.commit()

    def __iter__(self):
        return self.cursor.__iter__()


class LmdbWriteBatch(PairWriteBatch):
    def __init__(self, adapter: LmdbAdapter, txn):
        L.trace(f'creating lmdb write batch of env={adapter.path}')
        self.adapter = adapter
        self.txn = txn

    def get(self, k):
        ret = None
        try:
            ret = self.txn.get(k)
        except lmdb.BadValsizeError as e:
            raise ValueError(f"get key={k} raise lmdb.BadValsizeError")
        except:
            raise ValueError(f"get key={k} raise Exception")
        return ret

    def put(self, k, v):
        ret = None
        try:
            ret = self.txn.put(k, v)
        except lmdb.BadValsizeError as e:
            raise ValueError(f"put key={k}, value={v} raise lmdb.BadValsizeError")
        except:
            raise ValueError(f"put key={k}, value={v} raise Exception")
        return ret

    def merge(self, merge_func, k, v):
        old_value = self.txn.get(k)
        if old_value is not None:
            new_value = merge_func(old_value, v)
        else:
            new_value = v

        ret = None
        try:
            ret = self.txn.put(k, new_value)
        except lmdb.BadValsizeError as e:
            L.info(f"put key={k}, value={new_value} raise lmdb.BadValsizeError")
        except:
            L.info(f"put key={k}, value={new_value} raise Exception")
        return ret

    def delete(self, k):
        self.txn.delete(k)

    def write(self):
        pass

    def close(self):
        pass
