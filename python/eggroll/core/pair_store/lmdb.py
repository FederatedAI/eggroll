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
import platform
import os
import threading

import lmdb

from eggroll.core.pair_store.adapter import PairIterator, PairWriteBatch, \
    PairAdapter
from eggroll.utils.log_utils import get_logger

L = get_logger()

#64 * 1024 * 1024
LMDB_MAP_SIZE = 16 * 4_096 * 244_140    # follows storage-service-cxx's config here
LMDB_MAP_SIZE_WINDOWS = 200 * 1024 * 1024 * 10
DEFAULT_DB = b'main'
# DELIMETER = '-'
# DELIMETER_ENCODED = DELIMETER.encode()

class LmdbIterator(PairIterator):
    def __init__(self, adapter):
        L.info("create lmdb iterator")
        self.adapter = adapter
        self.cursor = adapter.txn_r.cursor()

    #move cursor to the first key position
    #return True if success or False if db is empty
    def first(self):
        return self.cursor.first()

    #same as first() but last key position
    def last(self):
        return self.cursor.last()

    #return the current key
    def key(self):
        return self.cursor.key()

    def close(self):
        self.cursor.close()

    def __iter__(self):
        return self.cursor.__iter__()

class LmdbWriteBatch(PairWriteBatch):

    def __init__(self, adapter, txn):
        self.adapter = adapter
        self.txn = txn

    def put(self, k, v):
        self.txn.put(k, v)

    def delete(self, k):
        self.txn.delete(k)

    def write(self):
        pass

    def close(self):
        pass


class LmdbAdapter(PairAdapter):
    env_lock = threading.Lock()
    env_dict = dict()
    count_dict = dict()
    sub_env_dict = dict()
    txn_r_dict = dict()
    txn_w_dict = dict()

    def get(self, key):
        return self.cursor.get(key)

    def put(self, key, value):
        return self.txn_w.put(key, value)

    def __init__(self, options):
        with LmdbAdapter.env_lock:
            L.info("lmdb adapter init")
            super().__init__(options)
            self.path = options["path"]
            lmdb_map_size = options.get("lmdb_map_size", LMDB_MAP_SIZE if platform.system() != 'Windows' else LMDB_MAP_SIZE_WINDOWS)
            create_if_missing = bool(options.get("create_if_missing", "True"))
            if self.path not in LmdbAdapter.env_dict:
                if create_if_missing:
                    os.makedirs(self.path, exist_ok=True)
                L.info("path not in dict db path:{}".format(self.path))
                writemap = False if platform.system() == 'Darwin' else True
                self.env = lmdb.open(self.path, create=create_if_missing, max_dbs=128, sync=False, map_size=lmdb_map_size, writemap=writemap)
                self.sub_db = self.env.open_db(DEFAULT_DB)
                self.txn_r = self.env.begin(db=self.sub_db, write=False)
                self.txn_w = self.env.begin(db=self.sub_db, write=True)
                LmdbAdapter.count_dict[self.path] = 0
                LmdbAdapter.env_dict[self.path] = self.env
                LmdbAdapter.sub_env_dict[self.path] = self.sub_db
                LmdbAdapter.txn_r_dict[self.path] = self.txn_r
                LmdbAdapter.txn_w_dict[self.path] = self.txn_w
            else:
                L.info("path in dict:{}".format(self.path))
                self.env = LmdbAdapter.env_dict[self.path]
                self.sub_db = LmdbAdapter.sub_env_dict[self.path]
                self.txn_r = LmdbAdapter.txn_r_dict[self.path]
                self.txn_w = LmdbAdapter.txn_w_dict[self.path]
            self.cursor = self.txn_r.cursor()
            LmdbAdapter.count_dict[self.path] = LmdbAdapter.count_dict[self.path] + 1

    def __enter__(self):
        return self
    # TODO:0: duplicated code， lmdb.Error: Attempt to operate on closed/deleted/dropped object.
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    def get_sub_db(self):
        return self.env.open_db(DEFAULT_DB)

    def close(self):
        with LmdbAdapter.env_lock:
            if self.env:
                count = LmdbAdapter.count_dict[self.path]
                if not count or count - 1 <= 0:
                    try:
                        self.txn_r.commit()
                        self.txn_w.commit()
                        self.cursor.close()
                        self.env.close()
                    except:
                        L.warning("txn commit or cursor, env have closed before")

                    del LmdbAdapter.env_dict[self.path]
                    del LmdbAdapter.sub_env_dict[self.path]
                    del LmdbAdapter.count_dict[self.path]
                    del LmdbAdapter.txn_r_dict[self.path]
                    del LmdbAdapter.txn_w_dict[self.path]
                else:
                    LmdbAdapter.count_dict[self.path] = count - 1
                self.env = None

    def iteritems(self):
        return LmdbIterator(self)

    def new_batch(self):
        return LmdbWriteBatch(self, self.txn_w)

    def count(self):
        return self.txn_r.stat()["entries"]

    def delete(self, k):
        return self.txn_w.delete(k)

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