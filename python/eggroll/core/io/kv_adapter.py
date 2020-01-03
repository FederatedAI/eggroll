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

from eggroll.utils.log_utils import get_logger

L = get_logger()

try:
  import rocksdb
except:
  L.warn("WRAN: failed to import rocksdb")

try:
  import lmdb
except:
  L.warn("WRAN: failed to import lmdb")

# LMDB_MAP_SIZE = 16 * 4_096 * 244_140        # follows storage-service-cxx's config here
LMDB_MAP_SIZE = 64 * 1024 * 1024        # follows storage-service-cxx's config here
DEFAULT_DB = b'main'
DELIMETER = '-'
DELIMETER_ENCODED = DELIMETER.encode()

class AdapterManager:
  pass

class SortedKvAdapter(object):
  """
  Sorted key value store adapter
  """
  def __init__(self, options):
    pass

  def __del__(self):
    pass

  def close(self):
    raise NotImplementedError()

  def iteritems(self):
    raise NotImplementedError()

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


class SortedKvWriteBatch:
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

class SortedKvIterator:
  def close(self):
    raise NotImplementedError()

  def __iter__(self):
    raise NotImplementedError()

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()

class LmdbIterator(SortedKvIterator):
  def __init__(self, adapter, cursor):
    L.info("create lmdb iterator")
    self.adapter = adapter
    self.cursor = cursor

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
    pass

  def __iter__(self):
    return self.cursor.__iter__()

class LmdbWriteBatch(SortedKvWriteBatch):

  def __init__(self, adapter, txn):
    self.adapter = adapter
    self.txn = txn

  def put(self, k, v):
    self.txn.put(k, v)

  def delete(self, k, v):
    self.txn.delete(k, v)

  def write(self):
    pass

  def close(self):
    pass


class LmdbSortedKvAdapter(SortedKvAdapter):
  env_lock = threading.Lock()
  env_dict = dict()
  count_dict = dict()
  sub_env_dict = dict()
  txn_dict = dict()

  def get(self, key):
    return self.cursor.get(key)

  def put(self, key, value):
    return self.txn.put(key, value)

  def __init__(self, options):
    with LmdbSortedKvAdapter.env_lock:
      L.info("lmdb adapter init")
      super().__init__(options)
      self.path = options["path"]
      create_if_missing = bool(options.get("create_if_missing", "True"))
      if self.path not in LmdbSortedKvAdapter.env_dict:
        if create_if_missing:
            os.makedirs(self.path, exist_ok=True)
        L.info("path not in dict db path:{}".format(self.path))
        self.env = lmdb.open(self.path, create=create_if_missing, max_dbs=128, sync=False, map_size=LMDB_MAP_SIZE, writemap=True)
        self.sub_db = self.env.open_db(DEFAULT_DB)
        self.txn = self.env.begin(db=self.sub_db, write=True)
        LmdbSortedKvAdapter.count_dict[self.path] = 0
        LmdbSortedKvAdapter.env_dict[self.path] = self.env
        LmdbSortedKvAdapter.sub_env_dict[self.path] = self.sub_db
        LmdbSortedKvAdapter.txn_dict[self.path] = self.txn
      else:
        L.info("path in dict:{}".format(self.path))
        self.env = LmdbSortedKvAdapter.env_dict[self.path]
        self.sub_db = LmdbSortedKvAdapter.sub_env_dict[self.path]
        self.txn = LmdbSortedKvAdapter.txn_dict[self.path]
      self.cursor = self.txn.cursor()
      LmdbSortedKvAdapter.count_dict[self.path] = LmdbSortedKvAdapter.count_dict[self.path] + 1

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()
    with LmdbSortedKvAdapter.env_lock:
      if self.env:
        count = LmdbSortedKvAdapter.count_dict[self.path]
        if not count or count - 1 <= 0:
          LmdbSortedKvAdapter.txn_dict[self.path].commit()
          LmdbSortedKvAdapter.env_dict[self.path].close()
          del LmdbSortedKvAdapter.txn_dict[self.path]
          del LmdbSortedKvAdapter.env_dict[self.path]
          del LmdbSortedKvAdapter.sub_env_dict[self.path]
          del LmdbSortedKvAdapter.count_dict[self.path]
        else:
          LmdbSortedKvAdapter.count_dict[self.path] = count - 1
        self.env = None

  def __del__(self):
    with LmdbSortedKvAdapter.env_lock:
      if self.env:
        count = LmdbSortedKvAdapter.count_dict[self.path]
        if not count or count - 1 <= 0:
          del LmdbSortedKvAdapter.env_dict[self.path]
          del LmdbSortedKvAdapter.sub_env_dict[self.path]
          del LmdbSortedKvAdapter.txn_dict[self.path]
          del LmdbSortedKvAdapter.count_dict[self.path]
        else:
          LmdbSortedKvAdapter.count_dict[self.path] = count - 1

  def get_sub_db(self):
    return self.env.open_db(DEFAULT_DB)

  def close(self):
    try:
      self.txn.commit()
      self.cursor.close()
    except:
      L.warning("txn has closed")

  def iteritems(self):
    return LmdbIterator(self, self.cursor)

  def new_batch(self):
    return LmdbWriteBatch(self, self.txn)

  def count(self):
    return self.txn.stat()["entries"]
    #return self.cursor.count()

  def delete(self, k):
    self.txn.delete(k)

class RocksdbWriteBatch(SortedKvWriteBatch):
  def __init__(self, adapter, chunk_size=100000):
    self.chunk_size = chunk_size
    self.batch = rocksdb.WriteBatch()
    self.adapter = adapter
    self.key = None
    self.value = None

  def put(self, k, v):
    self.key = k
    self.value = v
    self.batch.put(k, v)
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

class RocksdbIterator(SortedKvIterator):
  def __init__(self, adapter):
    self.adapter = adapter
    self.it = adapter.db.iteritems()
    self.it.seek_to_first()

  def first(self):
    count = 0
    self.it.seek_to_first()
    for k, v in self.it:
      count += 1
    self.it.seek_to_first()
    return (count != 0)

  def last(self):
    count = 0
    self.it.seek_to_last()
    for k, v in self.it:
      count += 1
    self.it.seek_to_last()
    return (count != 0)

  def key(self):
    return self.it.get()[0]

  def close(self):
    pass

  def __iter__(self):
    return self.it

class RocksdbSortedKvAdapter(SortedKvAdapter):
  env_lock = threading.Lock()
  env_dict = dict()
  count_dict = dict()

  def __init__(self, options):
    """
    :param options:
      path: absolute local fs path
      create_if_missing: default true
    """
    with RocksdbSortedKvAdapter.env_lock:
      super().__init__(options)
      self.path = options["path"]
      opts = rocksdb.Options()
      opts.create_if_missing = bool(options.get("create_if_missing", "True"))
      opts.compression = rocksdb.CompressionType.no_compression
      if self.path not in RocksdbSortedKvAdapter.env_dict:
        if opts.create_if_missing:
            os.makedirs(self.path, exist_ok=True)
        self.db = rocksdb.DB(self.path, opts)
        L.info("path not in dict db path:{}".format(self.path))
        RocksdbSortedKvAdapter.count_dict[self.path] = 0
        RocksdbSortedKvAdapter.env_dict[self.path] = self.db
      else:
        L.info("path in dict:{}".format(self.path))
        self.db = RocksdbSortedKvAdapter.env_dict[self.path]
    RocksdbSortedKvAdapter.count_dict[self.path] = RocksdbSortedKvAdapter.count_dict[self.path] + 1

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    L.info("exit")
    with RocksdbSortedKvAdapter.env_lock:
      if self.db:
        count = RocksdbSortedKvAdapter.count_dict[self.path]
        if not count or count - 1 <= 0:
          del RocksdbSortedKvAdapter.env_dict[self.path]
          del RocksdbSortedKvAdapter.count_dict[self.path]
        else:
          RocksdbSortedKvAdapter.count_dict[self.path] = RocksdbSortedKvAdapter.count_dict[self.path] - 1
    self.close()

  def __del__(self):
    with RocksdbSortedKvAdapter.env_lock:
      if hasattr(self, 'db'):
        count = RocksdbSortedKvAdapter.count_dict[self.path]
        if not count or count - 1 <= 0:
          del RocksdbSortedKvAdapter.env_dict[self.path]
          del RocksdbSortedKvAdapter.count_dict[self.path]
        else:
          RocksdbSortedKvAdapter.count_dict[self.path] = count - 1

  def get(self, key):
    return self.db.get(key)

  def put(self, key, value):
    self.db.put(key, value)

  def close(self):
    if self.db:
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
